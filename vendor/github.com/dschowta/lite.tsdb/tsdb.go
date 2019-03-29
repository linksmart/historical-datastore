package tsdb

import (
	"fmt"
	"os"
	"sync"
)

const (
	ASC  = "asc"
	DESC = "desc"
)

type BoltDBConfig struct {
	Path string
	Mode os.FileMode
}

type TimeEntry struct {
	Time  int64
	Value []byte
}

type Query struct {
	Series string

	Start int64
	End   int64
	//Sorting order:
	//Possible values are ASC and DESC
	//ASC : The time Series will have the oldest data first
	//DESC: The time Series will have the latest  data first.
	Sort string

	//Number of entries to be returned per page. This is used for pagination.
	// The next sequence is found out using NextEntry variable of a query response.
	Limit int
}

type TimeSeries []TimeEntry

type TSDB interface {

	//This function adds the senml records
	Add(name string, timeseries TimeSeries) error

	//Get the senml records
	Query(q Query) (timeSeries TimeSeries, nextEntry *int64, err error)

	QueryOnChannel(q Query) (timeseries <-chan TimeEntry, nextEntry chan *int64, err chan error)
	//Get the total pages for a particular query.
	// This helps for any client to call multiple queries
	GetPages(q Query) (seriesList []int64, count int, err error)

	//Get the senml records
	Get(series string) (timeSeries TimeSeries, err error)
	//Returns two channels, one for Time entries and one for error.
	//This avoids the usage of an extra buffer by the database
	//Caution: first read the channel and then read the error. Error channel shall be written only after the timeseries channel is closed
	GetOnChannel(series string) (timeseries <-chan TimeEntry, err chan error)

	//Delete a complete Series
	Delete(series string) error

	//Close the database
	Close() error
}

var ds TSDB        //will be used as a singleton db object
var once sync.Once //make thread safe singleton

func Open(config interface{}) (TSDB, error) {
	switch config.(type) {
	case BoltDBConfig:
		retDB := new(Boltdb)
		err := retDB.open(config.(BoltDBConfig))
		return retDB, err
	default:
		return nil, fmt.Errorf("Unsupported storage Configuration")
	}
}
