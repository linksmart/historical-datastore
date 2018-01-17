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

type InfluxAggr struct {
	influxStorage *data.InfluxStorage
}

func NewInfluxAggr(influxStorage *data.InfluxStorage) (Storage, chan<- common.Notification, error) {

	a := &InfluxAggr{
		influxStorage: influxStorage,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListener(a, ntChan)

	return a, ntChan, nil
}

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
			aggr.Aggregates[0], a.fqMsrmt(ds.ID, aggr.ID), timeCond)
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error counting records for source %v: %s", ds.Resource, err)
		}
		if count < 1 {
			logger.Printf("There is no data for source %v", ds.Resource)
			continue
		}
		total += int(count)

		res, err := a.influxStorage.QuerySprintf("SELECT * FROM %s WHERE %s ORDER BY time %s LIMIT %d OFFSET %d",
			a.fqMsrmt(ds.ID, aggr.ID), timeCond, sort, perItems[i], offsets[i])
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error retrieving aggregated data records for source %v: %s", ds.Resource, err)
		}

		if len(res[0].Series) > 1 {
			return DataSet{}, 0, logger.Errorf("Unrecognized/Corrupted database schema.")
		}

		pds, err := pointsFromRow(res[0].Series[0], aggr, ds)
		if err != nil {
			return DataSet{}, 0, logger.Errorf("Error parsing records for source %v: %s", ds.Resource, err)
		}

		if perItems[i] != 0 { // influx ignores `limit 0`
			points = append(points, pds...)
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

// Handles the creation of a new data source
func (a *InfluxAggr) NtfCreated(ds registry.DataSource, callback chan error) {

	for _, dsa := range ds.Aggregation {
		err := a.createRetentionPolicy(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
		err = a.createContinuousQuery(ds, dsa)
		if err != nil {
			a.dropRetentionPolicy(ds, dsa)
			callback <- err
			return
		}
	}

	callback <- nil
}

// Handles updates of a data source
func (a *InfluxAggr) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	aggrs := make(map[string]registry.Aggregation)
	for _, dsa := range oldDS.Aggregation {
		aggrs[dsa.ID] = dsa
	}

	for _, dsa := range newDS.Aggregation {
		oldAds, found := aggrs[dsa.ID]

		// NEW AGGREGATION
		if !found {
			// Create Retention Policy
			err := a.createRetentionPolicy(newDS, dsa)
			if err != nil {
				callback <- err
				return
			}
			// Create Continuous Query
			err = a.createContinuousQuery(newDS, dsa)
			if err != nil {
				a.dropRetentionPolicy(newDS, dsa)
				callback <- err
				return
			}
			// Backfill
			err = a.backfill(newDS, dsa)
			if err != nil {
				callback <- err
				return
			}

			continue
		}

		// UPDATED AGGREGATION
		if oldAds.Retention != dsa.Retention {
			// Alter Retention Policy
			err := a.alterRetentionPolicy(oldDS, dsa)
			if err != nil {
				callback <- err
				return
			}
		}

		delete(aggrs, dsa.ID)
	}

	// DELETED AGGREGATIONS
	for _, dsa := range aggrs {
		// Drop Continuous Query
		err := a.dropContinuousQuery(oldDS, dsa)
		if err != nil {
			callback <- err
			return
		}
		// Drop Measurement
		err = a.dropMeasurement(oldDS, dsa)
		if err != nil {
			callback <- err
			return
		}
		// Drop Retention Policy
		err = a.dropRetentionPolicy(oldDS, dsa)
		if err != nil {
			callback <- err
			return
		}
	}

	callback <- nil
}

// Handles deletion of a data source
func (a *InfluxAggr) NtfDeleted(ds registry.DataSource, callback chan error) {

	for _, dsa := range ds.Aggregation {
		// Drop Continuous Query
		err := a.dropContinuousQuery(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
		// Drop Measurement
		err = a.dropMeasurement(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
		// Drop Retention Policy
		err = a.dropRetentionPolicy(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
	}

	callback <- nil
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

// Formatted continuous query name for a given data source
func (a *InfluxAggr) cq(dsID, aggrID string) string {
	return fmt.Sprintf("\"cq_%s_%s\"", aggrID, dsID)
}

// Formatted retention policy name for a given data source
func (a *InfluxAggr) retention(dsID, aggrID string) string {
	return fmt.Sprintf("\"aggr_policy_%s_%s\"", aggrID, dsID)
}

// Formatted measurement name for a given data source
func (a *InfluxAggr) msrmt(dsID, aggrID string) string {
	return fmt.Sprintf("\"aggr_%s_%s\"", aggrID, dsID)
}

// Fully qualified measurement name
func (a *InfluxAggr) fqMsrmt(dsID, aggrID string) string {
	return fmt.Sprintf("%s.%s.%s", a.influxStorage.Database(), a.retention(dsID, aggrID), a.msrmt(dsID, aggrID))
}

// e.g. returned string: min(value),max(value)
func (a *InfluxAggr) functions(dsa registry.Aggregation) string {
	var funcs []string
	for _, aggr := range dsa.Aggregates {
		funcs = append(funcs, fmt.Sprintf("%s(value)", aggr))
	}
	return strings.Join(funcs, ",")
}

// INFLUX QUERIES

// Backfill aggregates for the historical data
func (a *InfluxAggr) backfill(ds registry.DataSource, dsa registry.Aggregation) error {
	const SINCE int = -10 // years ago
	now := time.Now().UTC()
	// backfill one year at a time
	for s := 0; s > SINCE; s-- {
		_, err := a.influxStorage.QuerySprintf("SELECT %s INTO %s FROM %s WHERE time >= '%v' AND time < '%v' GROUP BY time(%s) fill(none)",
			a.functions(dsa), a.fqMsrmt(ds.ID, dsa.ID), a.influxStorage.FQMsrmt(&ds),
			now.AddDate(s-1, 0, 0).Format("2006-01-02 15:04:05"), now.AddDate(s, 0, 0).Format("2006-01-02 15:04:05"),
			dsa.Interval)
		if err != nil {
			return logger.Errorf("Error backfilling aggregates: %s", err)
		}
	}

	logger.Printf("InfluxAggr: backfilled aggregates of %s/%s for %d years.", dsa.ID, ds.ID, SINCE)
	return nil
}

func (a *InfluxAggr) createRetentionPolicy(ds registry.DataSource, dsa registry.Aggregation) error {
	duration := "INF"
	if dsa.Retention != "" {
		duration = dsa.Retention
	}

	_, err := a.influxStorage.QuerySprintf("CREATE RETENTION POLICY %s ON %s DURATION %v REPLICATION %d",
		a.retention(ds.ID, dsa.ID), a.influxStorage.Database(), duration, a.influxStorage.Replication())
	if err != nil {
		if strings.Contains(err.Error(), "retention policy already exists") {
			logger.Printf("WARNING: %s: %v", err, a.retention(ds.ID, dsa.ID))
			return nil
		}
		return logger.Errorf("Error creating retention policy: %s", err)
	}
	logger.Printf("InfluxAggr: created retention policy %s/%s", dsa.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) createContinuousQuery(ds registry.DataSource, dsa registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("CREATE CONTINUOUS QUERY %s ON %s BEGIN SELECT %s INTO %s FROM %s GROUP BY time(%s) fill(none) END",
		a.cq(ds.ID, dsa.ID), a.influxStorage.Database(), a.functions(dsa), a.fqMsrmt(ds.ID, dsa.ID), a.influxStorage.FQMsrmt(&ds), dsa.Interval)
	if err != nil {
		if strings.Contains(err.Error(), "continuous query already exists") {
			logger.Printf("WARNING: %s: %v", err, a.cq(ds.ID, dsa.ID))
			return nil
		}
		return logger.Errorf("Error creating aggregation: %s", err)
	}
	logger.Printf("InfluxAggr: created continuous query %s/%s", dsa.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) alterRetentionPolicy(ds registry.DataSource, dsa registry.Aggregation) error {
	duration := "INF"
	if dsa.Retention != "" {
		duration = dsa.Retention
	}
	// Setting SHARD DURATION 0s tells influx to use the default duration
	// https://docs.influxdata.com/influxdb/v1.2/query_language/database_management/#retention-policy-management
	_, err := a.influxStorage.QuerySprintf("ALTER RETENTION POLICY %s ON %s DURATION %v SHARD DURATION 0s",
		a.retention(ds.ID, dsa.ID), a.influxStorage.Database(), duration)
	if err != nil {
		return logger.Errorf("Error modifying retention: %s", err)
	}
	logger.Printf("InfluxAggr: altered retention policy %s/%s", dsa.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) dropRetentionPolicy(ds registry.DataSource, dsa registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("DROP RETENTION POLICY %s ON %s", a.retention(ds.ID, dsa.ID), a.influxStorage.Database())
	if err != nil {
		if strings.Contains(err.Error(), "retention policy not found") {
			logger.Printf("WARNING: %s: %v", err, a.retention(ds.ID, dsa.ID))
			return nil
		}
		return logger.Errorf("Error removing retention: %s", err)
	}
	logger.Printf("InfluxAggr: dropped retention policy %s/%s", ds.ID, dsa.ID)
	return nil
}

func (a *InfluxAggr) dropContinuousQuery(ds registry.DataSource, dsa registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("DROP CONTINUOUS QUERY %s ON %s", a.cq(ds.ID, dsa.ID), a.influxStorage.Database())
	if err != nil {
		if strings.Contains(err.Error(), "continuous query not found") {
			logger.Printf("WARNING: %s: %v", err, a.cq(ds.ID, dsa.ID))
			return nil
		}
		return logger.Errorf("Error dropping continuous query: %s", err)
	}
	logger.Printf("InfluxAggr: dropped continuous query %s/%s", dsa.ID, ds.ID)
	return nil
}

func (a *InfluxAggr) dropMeasurement(ds registry.DataSource, dsa registry.Aggregation) error {
	_, err := a.influxStorage.QuerySprintf("DROP MEASUREMENT %s", a.msrmt(ds.ID, dsa.ID))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			return nil
		}
		return logger.Errorf("Error removing historical data: %s", err)
	}
	logger.Printf("InfluxAggr: dropped measurement %s/%s", dsa.ID, ds.ID)
	return nil
}