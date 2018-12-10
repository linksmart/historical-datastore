// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package aggregation

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/influxdata/influxdb/models"
)

// InfluxAggr implements a InfluxDB aggregation client for HDS Data API
type InfluxAggr struct {
	influxStorage *data.InfluxStorage
}

// NewInfluxAggr returns a new InfluxAggr
func NewInfluxAggr(influxStorage *data.InfluxStorage) (Storage, error) {

	a := &InfluxAggr{
		influxStorage: influxStorage,
	}

	return a, nil
}

// Query retrieves aggregated data
func (a *InfluxAggr) Query(aggr registry.Aggregation, q data.Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
	points := []DataEntry{}
	total := 0

	// Set minimum time to 1970-01-01T00:00:00Z
	if q.Start.Before(time.Unix(0, 0)) {
		q.Start = time.Unix(0, 0)
		if q.End.Before(time.Unix(0, 1)) {
			return DataSet{}, 0, logger.Errorf("%s argument must be greater than 1970-01-01T00:00:00Z", common.ParamEnd)
		}
	}

	// If q.End is not set, make the query open-ended
	var timeCond string
	if q.Start.Before(q.End) {
		timeCond = fmt.Sprintf("time > '%s' AND time < '%s'", q.Start.Format(time.RFC3339), q.End.Format(time.RFC3339))
	} else {
		timeCond = fmt.Sprintf("time > '%s'", q.Start.Format(time.RFC3339))
	}

	perItems, offsets := common.PerItemPagination(q.Limit, page, perPage, len(sources))

	// Initialize sort order
	sort := "ASC"
	if q.Sort == common.DESC {
		sort = "DESC"
	}

	for i, ds := range sources {
		// Count total
		count, err := a.influxStorage.CountSprintf("SELECT COUNT(%s) FROM %s WHERE %s",
			aggr.Aggregates[0], a.measurementNameFQ(aggr.Retention, a.measurementName(ds.ID, aggr.ID)), timeCond)
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error counting records for source %v: %s", ds.Resource, err)
		}
		if count < 1 {
			logger.Printf("There is no data for source %v", ds.Resource)
			continue
		}
		total += int(count)

		res, err := a.influxStorage.QuerySprintf("SELECT * FROM %s WHERE %s ORDER BY time %s LIMIT %d OFFSET %d",
			a.measurementNameFQ(aggr.Retention, a.measurementName(ds.ID, aggr.ID)),
			timeCond,
			sort,
			perItems[i],
			offsets[i])
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error retrieving aggregated data records for source %v: %s", ds.Resource, err)
		}

		if len(res[0].Series) > 1 {
			return DataSet{}, 0, logger.Errorf("Unrecognized/Corrupted database schema.")
		}

		rowPoints, err := pointsFromRow(res[0].Series[0], aggr, ds)
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error parsing records for source %v: %s", ds.Resource, err)
		}

		if perItems[i] != 0 { // influx ignores `limit 0`
			points = append(points, rowPoints...)
		}
	}
	var dataset DataSet
	dataset.Entries = points

	// q.Limit overrides total
	if q.Limit > 0 && q.Limit < total {
		total = q.Limit
	}

	return dataset, total, nil
}

// CreateHandler handles the creation of a new data source
func (a *InfluxAggr) CreateHandler(ds registry.DataSource) error {

	for _, aggr := range ds.Aggregation {

		err := a.createContinuousQuery(ds, aggr)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateHandler handles updates of a data source
func (a *InfluxAggr) UpdateHandler(oldDS registry.DataSource, newDS registry.DataSource) error {

	oldAggrs := make(map[string]registry.Aggregation)
	for _, aggr := range oldDS.Aggregation {
		oldAggrs[aggr.ID] = aggr
	}

	for _, newAggr := range newDS.Aggregation {
		oldAggr, found := oldAggrs[newAggr.ID]

		// NEW AGGREGATION
		if !found {
			// Create Continuous Query
			err := a.createContinuousQuery(newDS, newAggr)
			if err != nil {
				return err
			}
			// Backfill
			err = a.backfill(newDS, newAggr)
			if err != nil {
				return err
			}

			continue
		}

		// UPDATED AGGREGATION
		// modified retention
		if oldAggr.Retention != newAggr.Retention {
			logger.Debugf("Changing aggr retention from %s to %s", oldAggr.Retention, newAggr.Retention)
			// move aggr data to new RP
			err := a.influxStorage.ChangeRetentionPolicy(a.measurementName(oldDS.ID, oldAggr.ID), oldAggr.Aggregates[0], oldAggr.Retention, newAggr.Retention)
			if err != nil {
				return logger.Errorf("Error moving the historical aggregated data to new retention policy: %s", err)
			}
			// drop old CQ
			err = a.dropContinuousQuery(oldDS, oldAggr)
			if err != nil {
				return err
			}
			// create new CQ
			err = a.createContinuousQuery(newDS, newAggr)
			if err != nil {
				return err
			}
			// backfill in case retention has increased
			oldDur, _ := a.influxStorage.ParseDuration(oldAggr.Retention)
			newDur, _ := a.influxStorage.ParseDuration(newAggr.Retention)
			if newDur > oldDur {
				err = a.backfill(newDS, newAggr)
				if err != nil {
					return err
				}
			}
		}

		delete(oldAggrs, newAggr.ID)
	}

	// DELETED AGGREGATIONS
	for _, oldAggr := range oldAggrs {
		// Drop Continuous Query
		err := a.dropContinuousQuery(oldDS, oldAggr)
		if err != nil {
			return err
		}
		// Drop Measurement
		err = a.dropMeasurement(oldDS, oldAggr)
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteHandler handles deletion of a data source
func (a *InfluxAggr) DeleteHandler(ds registry.DataSource) error {

	for _, aggr := range ds.Aggregation {
		// Drop Continuous Query
		err := a.dropContinuousQuery(ds, aggr)
		if err != nil {
			return err
		}
		// Drop Measurement
		err = a.dropMeasurement(ds, aggr)
		if err != nil {
			return err
		}
	}

	return nil
}

// Utility functions

func pointsFromRow(r models.Row, aggr registry.Aggregation, ds registry.DataSource) ([]DataEntry, error) {
	var entries []DataEntry

	for _, e := range r.Values {
		entry := NewDataEntry()
		entry.Name = ds.Resource

		// fields and tags
		for i, v := range e {
			// point with nil column
			if v == nil {
				continue
			}
			switch r.Columns[i] {
			case "time":
				if val, ok := v.(string); ok {
					t, err := time.Parse(time.RFC3339, val)
					if err != nil {
						return nil, logger.Errorf("Invalid time format %v: %s", val, err)
					}
					entry.TimeStart = t.Unix()
					dur, err := time.ParseDuration(aggr.Interval)
					if err != nil {
						return nil, logger.Errorf("Invalid aggregation interval %v: %s", aggr.Interval, err)
					}
					entry.TimeEnd = t.Add(dur).Unix()
				} else {
					return nil, logger.Errorf("Interface conversion error. time not string?")
				}
			default:
				if common.SupportedAggregate(r.Columns[i]) {
					if val, err := v.(json.Number).Float64(); err == nil {
						entry.Aggregates[r.Columns[i]] = val
					} else {
						return nil, logger.Errorf("%s", err)
					}
				}
			} // endswitch

		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// cqName returns formatted continuous query name for a given data source
func (a *InfluxAggr) cqName(dsID, aggrID string) string {
	return fmt.Sprintf("\"cq_%s_%s\"", aggrID, dsID)
}

// measurementName returns formatted measurement name for a given data source aggregation
func (a *InfluxAggr) measurementName(dsID, aggrID string) string {
	return fmt.Sprintf("aggr_%s_%s", aggrID, dsID)
}

// measurementNameFQ returns fully qualified measurement name
func (a *InfluxAggr) measurementNameFQ(retention, measurementName string) string {
	return fmt.Sprintf("\"%s\".\"%s\".\"%s\"",
		a.influxStorage.Database(),
		a.influxStorage.RetentionPolicyName(retention),
		measurementName)
}

// functions returned formatted function signatures. E.g. min(value),max(value)
func (a *InfluxAggr) functions(aggr registry.Aggregation) string {
	var funcs []string
	for _, aggr := range aggr.Aggregates {
		funcs = append(funcs, fmt.Sprintf("%s(value)", aggr))
	}
	return strings.Join(funcs, ",")
}

// INFLUX QUERIES

// Backfill aggregates for the historical data
func (a *InfluxAggr) backfill(ds registry.DataSource, aggr registry.Aggregation) error {
	const SINCE int = -10 // years ago
	now := time.Now().UTC()

	dur, _ := a.influxStorage.ParseDuration(ds.Retention)
	aggrDur, _ := a.influxStorage.ParseDuration(aggr.Retention)
	if aggrDur < dur {
		dur = aggrDur
	}
	dur -= time.Minute // reduce a minute to avoid overshooting the retention interval
	defer logger.Printf("InfluxAggr: backfilled aggregates of %s/%s for %s.", aggr.ID, ds.ID, dur+time.Minute)

	// backfill one year at a time
	for s := 0; s > SINCE; s-- {
		from := now.AddDate(s-1, 0, 0)
		to := now.AddDate(s, 0, 0)
		if from.Before(now.Add(-dur)) {
			from = now.Add(-dur)
		}
		if !to.After(from) {
			return nil
		}

		_, err := a.influxStorage.QuerySprintf("SELECT %s INTO %s FROM %s WHERE time >= '%v' AND time < '%v' GROUP BY time(%s) fill(none)",
			a.functions(aggr),
			a.measurementNameFQ(aggr.Retention, a.measurementName(ds.ID, aggr.ID)),
			a.influxStorage.MeasurementNameFQ(ds.Retention, a.influxStorage.MeasurementName(ds.ID)),
			from.Format(time.RFC3339), to.Format(time.RFC3339),
			aggr.Interval)
		if err != nil {
			return logger.Errorf("Error backfilling aggregates: %s", err)
		}
	}

	return nil
}

func (a *InfluxAggr) createContinuousQuery(ds registry.DataSource, aggr registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("CREATE CONTINUOUS QUERY %s ON %s BEGIN SELECT %s INTO %s FROM %s GROUP BY time(%s) fill(none) END",
		a.cqName(ds.ID, aggr.ID),
		a.influxStorage.Database(),
		a.functions(aggr),
		a.measurementNameFQ(aggr.Retention, a.measurementName(ds.ID, aggr.ID)),
		a.influxStorage.MeasurementNameFQ(ds.Retention, a.influxStorage.MeasurementName(ds.ID)),
		aggr.Interval)
	if err != nil {
		if strings.Contains(err.Error(), "continuous query already exists") {
			logger.Printf("WARNING: %s: %v", err, a.cqName(ds.ID, aggr.ID))
			return nil
		}
		return logger.Errorf("Error creating aggregation: %s", err)
	}
	logger.Printf("InfluxAggr: created continuous query %s/%s", aggr.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) dropContinuousQuery(ds registry.DataSource, aggr registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("DROP CONTINUOUS QUERY %s ON %s",
		a.cqName(ds.ID, aggr.ID),
		a.influxStorage.Database())
	if err != nil {
		if strings.Contains(err.Error(), "continuous query not found") {
			logger.Printf("WARNING: %s: %v", err, a.cqName(ds.ID, aggr.ID))
			return nil
		}
		return logger.Errorf("Error dropping continuous query: %s", err)
	}
	logger.Printf("InfluxAggr: dropped continuous query %s/%s", aggr.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) dropMeasurement(ds registry.DataSource, aggr registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("DROP MEASUREMENT \"%s\"", a.measurementName(ds.ID, aggr.ID))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			return nil
		}
		return logger.Errorf("Error removing historical data: %s", err)
	}
	logger.Printf("InfluxAggr: dropped measurement %s/%s", aggr.ID, ds.ID)
	return nil
}
