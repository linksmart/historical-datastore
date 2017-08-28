package aggregation

import (
	"errors"
	"fmt"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
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
	return DataSet{}, 0, ErrNotImplemented
}

func (a *MongoAggr) NtfCreated(ds registry.DataSource, callback chan error) {
	fmt.Println("ds:", ds)
	if len(ds.Aggregation) > 0 {
		callback <- errors.New("The storage backend does not support aggregation.")
	} else {
		callback <- nil
	}
}

func (a *MongoAggr) NtfUpdated(oldDS registry.DataSource, newDS registry.DataSource, callback chan error) {
	fmt.Println("ds:", newDS)
	if len(newDS.Aggregation) > 0 {
		callback <- errors.New("The storage backend does not support aggregation.")
	} else {
		callback <- nil
	}
}

func (a *MongoAggr) NtfDeleted(ds registry.DataSource, callback chan error) {
	callback <- nil
}
