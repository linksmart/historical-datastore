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

func validateCreation(ds DataStream, conf common.RegConf) error {
	var e validationError

	//validate name
	if ds.Name == "" {
		e.mandatory = append(e.mandatory, "name")
	}
	validSenmlName, err := regexp.Compile(`^[a-zA-Z0-9]+[a-zA-Z0-9-:./_]*$`)
	if err != nil {
		fmt.Println(err)
	}
	if !validSenmlName.MatchString(ds.Name) {
		e.invalid = append(e.invalid, "name")
	}

	//validate source

	if e.Err() {
		return e
	}
	return nil
}

func validateUpdate(ds DataStream, oldDS DataStream, conf common.RegConf) error {
	var e validationError

	// id
	if ds.Name != oldDS.Name {
		e.readOnly = append(e.readOnly, "id")
	}

	// type
	if ds.Type != oldDS.Type {
		e.readOnly = append(e.readOnly, "type")
	}

	//TODO: add validation logics
	/*

		// url
		if ds.BrokerURL != oldDS.BrokerURL {
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
		if !common.SupportedPeriod(ds.Retention) {
			e.invalid = append(e.invalid, "retention")
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
