// Package aggregation implements Aggregation API
package aggregation

import (
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)

type Aggr interface {
	// Queries data for specified data sources
	Query(q data.Query, page, perPage int, sources ...registry.DataSource) (data.DataSet, int, error)

	// Methods for handling notifications
	ntfCreated(ds registry.DataSource, callback chan error)
	ntfUpdated(old registry.DataSource, new registry.DataSource, callback chan error)
	ntfDeleted(ds registry.DataSource, callback chan error)
}
