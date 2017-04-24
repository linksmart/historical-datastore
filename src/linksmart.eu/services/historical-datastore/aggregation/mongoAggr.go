package aggregation

import (

)
import (
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

type MongoAggr struct {
	mongoStorage *data.MongoStorage
}

func NewMongoAggr(mongoStorage *data.MongoStorage) (Storage, chan<- common.Notification, error) {

	a := &MongoAggr{
		mongoStorage: mongoStorage,
	}

	// Run the notification listener
	ntChan := make(chan common.Notification)
	go NtfListener(a, ntChan)

	return a, ntChan, nil
}

func (a *MongoAggr) Query(aggr registry.Aggregation, q data.Query, page, perPage int, sources ...registry.DataSource) (DataSet, int, error) {
	return DataSet{}, 0, nil
}

func (a *MongoAggr) NtfCreated(ds registry.DataSource, callback chan error) {
	callback <- nil
}

func (a *MongoAggr) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {
	callback <-nil
}

func (a *MongoAggr) NtfDeleted(ds registry.DataSource, callback chan error) {
	callback <- nil
}