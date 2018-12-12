// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package aggregation

import (
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
)

type Storage interface {
	// Queries data for specified data sources
	Query(aggr registry.Aggregation, q data.Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error)

	// EventListener includes methods for event handling
	registry.EventListener
}
