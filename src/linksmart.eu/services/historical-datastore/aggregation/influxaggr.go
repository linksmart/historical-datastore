package aggregation

import (
	"fmt"
	"log"
	"strings"

	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)

type InfluxAggr struct {
	influxStorage *data.InfluxStorage
}

func NewInfluxAggr(influxStorage *data.InfluxStorage) (Aggr, chan<- common.Notification, error) {

	a := &InfluxAggr{
		influxStorage: influxStorage,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go ntListener(a, ntChan)

	return a, ntChan, nil
}

func (a *InfluxAggr) Query(q data.Query, page, perPage int, sources ...registry.DataSource) (data.DataSet, int, error) {
	// TODO
	return data.DataSet{}, 0, nil
}

// Formatted continuous query name for a given data source
func (a *InfluxAggr) cq(ds registry.DataSource, dsa registry.AggregatedDataSource) string {
	return fmt.Sprintf("\"cq_%s/%s\"", ds.ID, dsa.ID)
}

// Formatted retention policy name for a given data source
func (a *InfluxAggr) retention(ds registry.DataSource, dsa registry.AggregatedDataSource) string {
	return fmt.Sprintf("\"policy_%s/%s\"", ds.ID, dsa.ID)
}

// Formatted measurement name for a given data source
func (a *InfluxAggr) msrmt(ds registry.DataSource, dsa registry.AggregatedDataSource) string {
	return fmt.Sprintf("\"aggr_%s/%s\"", ds.ID, dsa.ID)
}

// Fully qualified measurement name
func (a *InfluxAggr) fqMsrmt(ds registry.DataSource, dsa registry.AggregatedDataSource) string {
	return fmt.Sprintf("%s.%s.%s", a.influxStorage.Database(), a.retention(ds, dsa), a.msrmt(ds, dsa))
}

// e.g. returned string: min(value),max(value)
func (a *InfluxAggr) functions(dsa registry.AggregatedDataSource) string {
	var funcs []string
	for _, aggr := range dsa.Aggregates {
		funcs = append(funcs, fmt.Sprintf("%s(value)", aggr))
	}
	return strings.Join(funcs, ",")
}

// Creates Retention Policy and Continuous Query
func (a *InfluxAggr) createAggregation(ds registry.DataSource, dsa registry.AggregatedDataSource) error {
	// Create Retention Policy
	duration := "INF"
	if dsa.Retention != "" {
		duration = dsa.Retention
	}
	_, err := a.influxStorage.QuerySprintf("CREATE RETENTION POLICY %s ON %s DURATION %v REPLICATION %d",
		a.retention(ds, dsa), a.influxStorage.Database(), duration, a.influxStorage.Replication())
	if err != nil {
		return fmt.Errorf("Error creating retention policy: %v", err.Error())
	}
	log.Printf("InfluxAggr: created retention policy %s/%s", ds.ID, dsa.ID)

	// Create Continuous Queries
	_, err = a.influxStorage.QuerySprintf("CREATE CONTINUOUS QUERY %s ON %s BEGIN SELECT %s INTO %s FROM %s GROUP BY time(%s) END",
		a.cq(ds, dsa), a.influxStorage.Database(), a.functions(dsa), a.fqMsrmt(ds, dsa), a.influxStorage.FQMsrmt(ds), dsa.Interval)
	if err != nil {
		return fmt.Errorf("Error creating aggregation: %v", err.Error())
	}
	log.Printf("InfluxAggr: created aggregation %s/%s", ds.ID, dsa.ID)
	return nil
}

// Drops Continuous Query, Measurement, and Retention Policy
func (a *InfluxAggr) deleteAggregation(ds registry.DataSource, dsa registry.AggregatedDataSource) error {
	// Drop Continuous Query
	_, err := a.influxStorage.QuerySprintf("DROP CONTINUOUS QUERY %s ON %s",
		a.cq(ds, dsa), a.influxStorage.Database())
	if err != nil {
		return fmt.Errorf("Error dropping aggregation: %v", err.Error())
	}
	log.Printf("InfluxAggr: dropped aggregation %s/%s", ds.ID, dsa.ID)

	// Drop Measurement
	_, err = a.influxStorage.QuerySprintf("DROP MEASUREMENT %s", a.msrmt(ds, dsa))
	if err != nil {
		if strings.Contains(err.Error(), "measurement not found") {
			// Not an error, No data to delete.
			goto DROP_RETENTION
		}
		return fmt.Errorf("Error removing the historical data: %v", err.Error())
	}
	log.Printf("InfluxStorage: dropped measurements for %s/%s", ds.ID, dsa.ID)

	// Drop Retention Policy
DROP_RETENTION:
	_, err = a.influxStorage.QuerySprintf("DROP RETENTION POLICY %s ON %s", a.retention(ds, dsa), a.influxStorage.Database())
	if err != nil {
		return fmt.Errorf("Error removing the retention policy for source: %v", err.Error())
	}
	log.Printf("InfluxStorage: dropped retention policy for %s/%s", ds.ID, dsa.ID)

	return nil
}

// Handles the creation of a new data source
func (a *InfluxAggr) ntfCreated(ds registry.DataSource, callback chan error) {

	for _, dsa := range ds.Aggregation {
		// Create Retention Policy
		// Create Continuous Queries
		err := a.createAggregation(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
	}

	callback <- nil
}

// Handles updates of a data source
func (a *InfluxAggr) ntfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {

	aggrs := make(map[string]registry.AggregatedDataSource)
	for _, dsa := range oldDS.Aggregation {
		aggrs[dsa.ID] = dsa
	}

	for _, dsa := range newDS.Aggregation {
		oldAds, found := aggrs[dsa.ID]

		// NEW AGGREGATION
		if !found {
			// Create Retention Policy
			// Create Continuous Queries
			err := a.createAggregation(newDS, dsa)
			if err != nil {
				callback <- err
				return
			}

			// Backfill aggregated for the historical data
			_, err = a.influxStorage.QuerySprintf("SELECT %s INTO %s FROM %s WHERE time >= '0001-01-01 00:00:00' GROUP BY time(%s)",
				a.functions(dsa), a.fqMsrmt(newDS, dsa), a.influxStorage.FQMsrmt(newDS), dsa.Interval)
			if err != nil {
				callback <- fmt.Errorf("Error backfilling aggregates: %v", err.Error())
				return
			}
			log.Printf("InfluxAggr: Backfilled aggregates %s/%s", newDS.ID, dsa.ID)

			continue
		}

		// UPDATED AGGREGATION
		if oldAds.Retention != dsa.Retention {
			// Alter retention
			duration := "INF"
			if dsa.Retention != "" {
				duration = dsa.Retention
			}
			_, err := a.influxStorage.QuerySprintf("ALTER RETENTION POLICY %s ON %s DURATION %v", a.retention(oldDS, dsa), a.influxStorage.Database(), duration)
			if err != nil {
				callback <- fmt.Errorf("Error modifying the retention policy for source: %v", err.Error())
				return
			}
			log.Printf("InfluxAggr: altered retention policy for aggregation %s/%s", oldDS.ID, dsa.ID)
		}

		delete(aggrs, dsa.ID)
	}

	// DELETED AGGREGATIONS
	for _, dsa := range aggrs {
		// Drop Continuous Query
		// Drop Measurement
		// Drop Retention Policy
		err := a.deleteAggregation(oldDS, dsa)
		if err != nil {
			callback <- err
			return
		}
	}

	callback <- nil
}

// Handles deletion of a data source
func (a *InfluxAggr) ntfDeleted(ds registry.DataSource, callback chan error) {

	for _, dsa := range ds.Aggregation {
		// Drop Continuous Query
		// Drop Measurement
		// Drop Retention Policy
		err := a.deleteAggregation(ds, dsa)
		if err != nil {
			callback <- err
			return
		}
	}

	callback <- nil
}
