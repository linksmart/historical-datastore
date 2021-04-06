// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/linksmart/historical-datastore/common"
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

func validateCreation(ts TimeSeries) error {
	var e validationError

	//validate name
	if ts.Name == "" {
		e.mandatory = append(e.mandatory, "name")
	}
	validSenmlName, err := regexp.Compile(`^[a-zA-Z0-9]+[a-zA-Z0-9-:./_]*$`)
	if err != nil {
		fmt.Println(err)
	}
	if !validSenmlName.MatchString(ts.Name) {
		e.invalid = append(e.invalid, "name")
	}

	//validate source

	if e.Err() {
		return e
	}
	return nil
}

func validateUpdate(ts TimeSeries, oldTS TimeSeries, conf common.RegConf) error {
	var e validationError

	// id
	if ts.Name != oldTS.Name {
		e.readOnly = append(e.readOnly, "id")
	}

	// type
	if ts.Type != oldTS.Type {
		e.readOnly = append(e.readOnly, "type")
	}

	//TODO: add validation logics
	/*

		// url
		if ts.BrokerURL != oldTS.BrokerURL {
			e.readOnly = append(e.readOnly, "url")
		}

		// data
		if ts.Data != oldTS.Data {
			e.readOnly = append(e.readOnly, "data")
		}

		// resource
		if ts.Resource != oldTS.Resource {
			e.readOnly = append(e.readOnly, "resource")
		}

		// retention
		if !common.SupportedPeriod(ts.Retention) {
			e.invalid = append(e.invalid, "retention")
		}



		// aggregation
		if ts.Type != common.FLOAT && len(ts.Aggregation) != 0 {
			e.other = append(e.other, "Aggregations are only possible with float type.")
		} else if ts.Type == common.FLOAT {
			for _, aggr := range ts.Aggregation {
				temp := aggr
				temp.Make(ts.ID)

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
				if !common.SupportedPeriod(aggr.Interval) {
					e.invalid = append(e.invalid, "aggregation.interval")
				}
				// retention
				if !common.SupportedPeriod(aggr.Retention) {
					e.invalid = append(e.invalid, "aggregation.retention")
				}
				// aggregates
				for _, aggregate := range aggr.Aggregates {
					if !common.SupportedAggregate(aggregate) {
						e.invalid = append(e.invalid, "aggregation.aggregate")
					}
				}
			}
		}


	*/
	if e.Err() {
		return e
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
	return len(e.readOnly)+len(e.mandatory)+len(e.invalid)+len(e.other) > 0
}
