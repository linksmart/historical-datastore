package registry

import (
	"errors"
	"time"

	"github.com/linksmart/historical-datastore/common"
)

// RESTful HTTP API
type Controller struct {
	s Storage
}

// Returns the configured TimeSeriesList API
func NewController(storage Storage) *Controller {
	return &Controller{
		s: storage,
	}
}

func (c Controller) getLastModifiedTime() (time.Time, common.Error) {
	t, err := c.s.getLastModifiedTime()
	if err != nil {
		return t, &common.InternalError{S: err.Error()}
	}
	return t, nil
}

func (c Controller) Add(ts TimeSeries) (*TimeSeries, common.Error) {
	addedTs, err := c.s.Add(ts)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrBadRequest) {
			return addedTs, err.(common.Error)
		} else {
			return addedTs, &common.InternalError{S: "error storing time series registry: " + err.Error()}
		}
	}
	return addedTs, nil
}
func (c Controller) Get(name string) (*TimeSeries, common.Error) {
	ts, err := c.s.Get(name)
	if errors.Is(err, ErrNotFound) {
		return ts, err.(common.Error)
	} else {
		return ts, &common.InternalError{S: "error retrieving time series: " + err.Error()}
	}
	return ts, nil
}

// Utility functions
func (c Controller) GetMany(page, perPage int) ([]TimeSeries, int, common.Error) {
	ts, total, err := c.s.GetMany(page, perPage)
	if err != nil {
		return ts, total, &common.InternalError{S: err.Error()}
	}
	return ts, total, nil
}
func (c Controller) FilterOne(path, op, value string) (*TimeSeries, common.Error) {
	ts, err := c.s.FilterOne(path, op, value)
	if err != nil {
		return ts, &common.InternalError{S: "Error processing the filter request:" + err.Error()}
	}
	return ts, nil
}
func (c Controller) Filter(path, op, value string, page, perPage int) ([]TimeSeries, int, common.Error) {
	ts, count, err := c.s.Filter(path, op, value, page, perPage)
	if err != nil {
		return ts, count, &common.InternalError{S: "Error processing the filter request:" + err.Error()}
	}
	return ts, count, nil
}

func (c Controller) Update(name string, ts TimeSeries) (*TimeSeries, common.Error) {
	t, err := c.s.Update(name, ts)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return t, err.(common.Error)
	} else {
		return t, &common.InternalError{S: "error updating time series: " + err.Error()}
	}
	return t, nil
}
func (c Controller) Delete(name string) common.Error {
	err := c.s.Delete(name)
	if errors.Is(err, ErrNotFound) {
		return err.(common.Error)
	} else {
		return &common.InternalError{S: "error deleting time series: " + err.Error()}
	}
}
