// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"net/url"
	"strings"

	"linksmart.eu/services/historical-datastore/common"
)

// DataSource writability:
// id: readonly
// url: readonly
// data: readonly
// resource: mandatory, fixed
// meta: n/a
// retention: n/a
// aggregation: id/data readonly
// type: mandatory, fixed
// format: mandatory

func validateCreation(ds DataSource) error {
	var e validationError

	// id
	if ds.ID != "" {
		e.readOnly = append(e.readOnly, "id")
	}

	// url
	if ds.URL != "" {
		e.readOnly = append(e.readOnly, "url")
	}

	// data
	if ds.Data != "" {
		e.readOnly = append(e.readOnly, "data")
	}

	// resource
	if ds.Resource == "" {
		e.mandatory = append(e.mandatory, "resource")
	}
	_, err := url.Parse(ds.Resource)
	if err != nil {
		e.invalid = append(e.invalid, "resource")
	}

	// retention
	if ds.Retention != "" {
		if !common.SupportedInterval(ds.Retention) {
			e.invalid = append(e.invalid, "retention")
		}
	}

	// type
	if ds.Type == "" {
		e.mandatory = append(e.mandatory, "type")
	}
	if !common.SupportedType(ds.Type) {
		e.invalid = append(e.invalid, "type")
	}

	// aggregation
	if ds.Type != common.FLOAT && len(ds.Aggregation) != 0 {
		e.other = append(e.other, "Aggregations are only possible with float type.")
	} else if ds.Type == common.FLOAT {
		for _, aggr := range ds.Aggregation {
			temp := aggr
			temp.Make(ds.ID)

			// Accept *correct* id and data attributes, even though they are readonly
			// id
			if aggr.ID != "" && aggr.ID != temp.ID {
				e.readOnly = append(e.readOnly, "aggregation.id")
			}
			// data
			if aggr.Data != "" && aggr.Data != temp.Data {
				e.readOnly = append(e.readOnly, "aggregation.data")
			}
			// interval
			if !common.SupportedInterval(aggr.Interval) {
				e.invalid = append(e.invalid, "aggregation.interval")
			}
			// retention
			if aggr.Retention != "" {
				if !common.SupportedInterval(aggr.Retention) {
					e.invalid = append(e.invalid, "aggregation.retention")
				}
			}
			// aggregates
			for _, aggregate := range aggr.Aggregates {
				if !common.SupportedAggregate(aggregate) {
					e.invalid = append(e.invalid, "aggregation.aggregate")
				}
			}
		}
	}

	// format
	if ds.Format == "" {
		e.mandatory = append(e.mandatory, "format")
	}

	if e.Err() {
		return e
	}
	return nil
}

func validateUpdate(ds DataSource, oldDS DataSource) error {
	var e validationError

	// id
	if ds.ID != oldDS.ID {
		e.readOnly = append(e.readOnly, "id")
	}

	// url
	if ds.URL != oldDS.URL {
		e.readOnly = append(e.readOnly, "url")
	}

	// data
	if ds.Data != oldDS.Data {
		e.readOnly = append(e.readOnly, "data")
	}

	// resource
	if ds.Resource != oldDS.Resource {
		e.readOnly = append(e.readOnly, "resource")
	}

	// retention
	if ds.Retention != "" {
		if !common.SupportedInterval(ds.Retention) {
			e.invalid = append(e.invalid, "retention")
		}
	}

	// type
	if ds.Type != oldDS.Type {
		e.readOnly = append(e.readOnly, "type")
	}

	// aggregation
	if ds.Type != common.FLOAT && len(ds.Aggregation) != 0 {
		e.other = append(e.other, "Aggregations are only possible with float type.")
	} else if ds.Type == common.FLOAT {
		for _, aggr := range ds.Aggregation {
			temp := aggr
			temp.Make(ds.ID)

			// Accept *correct* id and data attributes, even though they are readonly
			// id
			if aggr.ID != "" && aggr.ID != temp.ID {
				e.readOnly = append(e.readOnly, "aggregation.id")
			}
			// data
			if aggr.Data != "" && aggr.Data != temp.Data {
				e.readOnly = append(e.readOnly, "aggregation.data")
			}
			// interval
			if !common.SupportedInterval(aggr.Interval) {
				e.invalid = append(e.invalid, "aggregation.interval")
			}
			// retention
			if aggr.Retention != "" {
				if !common.SupportedInterval(aggr.Retention) {
					e.invalid = append(e.invalid, "aggregation.retention")
				}
			}
			// aggregates
			for _, aggregate := range aggr.Aggregates {
				if !common.SupportedAggregate(aggregate) {
					e.invalid = append(e.invalid, "aggregation.aggregate")
				}
			}
		}
	}

	// format
	if ds.Format == "" {
		e.mandatory = append(e.mandatory, "format")
	}

	if e.Err() {
		return logger.Errorf("%s", e)
	}
	return nil
}

// Custom error formatting
type validationError struct {
	readOnly  []string
	mandatory []string
	invalid   []string
	other     []string
}

func (e validationError) Error() string {
	var _errors []string
	if len(e.readOnly) > 0 {
		_errors = append(_errors, "Ambitious assignment to or modification of read-only attribute(s): "+strings.Join(e.readOnly, ", "))
	}
	if len(e.mandatory) > 0 {
		_errors = append(_errors, "Missing mandatory value(s) of: "+strings.Join(e.mandatory, ", "))
	}
	if len(e.invalid) > 0 {
		_errors = append(_errors, "Invalid value(s) for: "+strings.Join(e.invalid, ", "))
	}
	if len(e.other) > 0 {
		_errors = append(_errors, strings.Join(e.other, ", "))
	}
	return strings.Join(_errors, ". ")
}

func (e validationError) Err() bool {
	return len(e.readOnly)+len(e.mandatory)+len(e.invalid) > 0
}
