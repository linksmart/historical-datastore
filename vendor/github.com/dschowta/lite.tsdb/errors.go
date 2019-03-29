package tsdb

import "errors"

var (
	ErrSeriesNotFound = errors.New("timeseries not found")
)
