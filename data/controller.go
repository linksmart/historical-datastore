package data

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	errors2 "github.com/syndtr/goleveldb/leveldb/errors"
)

type Controller struct {
	registry         registry.Storage
	storage          Storage
	autoRegistration bool
}

// NewAPI returns the configured Data API
func NewController(registry registry.Storage, storage Storage, autoRegistration bool) *Controller {
	return &Controller{registry, storage, autoRegistration}
}

//TODO: Return right code in return so that right code is returned by callers. e.g. Grpc code or http error responses.
func (c Controller) submit(senmlPack senml.Pack, ids []string) common.Error {
	const Y3K = 32503680000 //Year 3000 BC, beyond which the time values are not taken
	//streams := make(map[string]*registry.DataStream)
	nameDSs := make(map[string]*registry.DataStream)
	fromStreamList := false
	if ids != nil {
		for _, id := range ids {
			ds, err := c.registry.Get(id)
			if err != nil {
				return &common.NotFoundError{S: fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", id, err)}
			}
			nameDSs[ds.Name] = ds
		}
		fromStreamList = true
	}

	// Fill the data map with provided data points
	data := make(map[string]senml.Pack)
	senmlPack.Normalize()
	for _, r := range senmlPack {
		// validate time. This is to make sure, timestamps are not set to precisions other than milliseconds.
		if r.Time > Y3K {
			return &common.BadRequestError{S: fmt.Sprintf("invalid senml entry %s: unix time value in seconds is too far in the future: %f", r.Name, r.Time)}
		}

		// search for the registry entry
		ds, found := nameDSs[r.Name]
		if !found && fromStreamList {
			return &common.BadRequestError{S: fmt.Sprintf("senml entry %s does not match the provided datastream", r.Name)}
		}
		if !found {
			var err error
			ds, err = c.registry.Get(r.Name)
			if err != nil {
				if errors.Is(err, errors2.ErrNotFound) {
					if !c.autoRegistration {
						return &common.NotFoundError{S: fmt.Sprintf("Data stream with name %v is not registered.", r.Name)}
					}

					// Register a Data stream with this name
					log.Printf("Registering Data stream for %s", r.Name)
					newDS := registry.DataStream{
						Name: r.Name,
						Unit: r.Unit,
					}
					if r.Value != nil || r.Sum != nil {
						newDS.Type = registry.Float
					} else if r.StringValue != "" {
						newDS.Type = registry.String
					} else if r.BoolValue != nil {
						newDS.Type = registry.Bool
					} else if r.DataValue != "" {
						newDS.Type = registry.Data
					}
					addedDS, err := c.registry.Add(newDS)
					if err != nil {
						return &common.BadRequestError{S: fmt.Sprintf("Error registering %v in the registry: %v", r.Name, err)}
					}
					ds = addedDS
				} else {
					return &common.InternalError{S: err.Error()}
				}
			}
			nameDSs[r.Name] = ds
		}

		err := validateRecordAgainstRegistry(r, ds)

		if err != nil {
			return &common.BadRequestError{S: fmt.Sprintf("Error validating the record:%v", err)}
		}

		// Prepare for storage
		_, found = data[ds.Name]
		if !found {
			data[ds.Name] = senml.Pack{}
		}
		data[ds.Name] = append(data[ds.Name], r)
	}

	// Add data to the storage
	err := c.storage.Submit(data, nameDSs)
	if err != nil {
		return &common.InternalError{S: "error writing data to the database: " + err.Error()}
	}
	return nil
}

func (c Controller) QueryPage(q Query, ids []string) (pack senml.Pack, total *int, retErr common.Error) {
	return c.queryStreamOrPage(q, ids, nil)
}
func (c Controller) QueryStream(q Query, ids []string, sendFunc SendFunction) (retErr common.Error) {
	_, _, retErr = c.queryStreamOrPage(q, ids, sendFunc)
	return retErr
}

func (c Controller) Count(q Query, streamNames []string) (total int, retErr common.Error) {
	var streams []*registry.DataStream
	for _, streamName := range streamNames {
		ds, err := c.registry.Get(streamName)
		if err != nil {
			return 0, &common.InternalError{S: fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", streamName, err)}
		}
		streams = append(streams, ds)
	}
	if len(streams) == 0 {
		return 0, &common.NotFoundError{S: "None of the specified Data streams could be retrieved from the registry."}
	}
	total, err := c.storage.Count(q, streams...)
	if err != nil {
		return 0, &common.InternalError{S: "Error retrieving count from the database: " + err.Error()}
	}
	return total, nil
}

func (c Controller) queryStreamOrPage(q Query, streamNames []string, sendFunc SendFunction) (pack senml.Pack, total *int, retErr common.Error) {
	var streams []*registry.DataStream
	for _, streamName := range streamNames {
		ds, err := c.registry.Get(streamName)
		if err != nil {
			return nil, nil, &common.InternalError{S: fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", streamName, err)}
		}
		streams = append(streams, ds)
	}

	if len(streams) == 0 {
		return nil, nil, &common.NotFoundError{S: "None of the specified Data streams could be retrieved from the registry."}
	}

	var err error
	if sendFunc == nil {
		pack, total, err = c.storage.QueryPage(q, streams...)
	} else {
		err = c.storage.QueryStream(q, sendFunc, streams...)
	}
	if err != nil {
		return nil, nil, &common.InternalError{S: "Error retrieving data from the database: " + err.Error()}
	}
	return pack, total, nil
}

func parseDenormParams(denormString string) (denormMask DenormMask, err error) {

	if denormString != "" {
		denormStrings := strings.Split(denormString, ",")
		for _, field := range denormStrings {
			switch strings.ToLower(strings.TrimSpace(field)) {
			case TimeField, TimeFieldShort:
				denormMask = denormMask | FTime
			case NameField, NameFieldShort:
				denormMask = denormMask | FName
			case UnitField, UnitFieldShort:
				denormMask = denormMask | FUnit
			case ValueField, ValueFieldShort:
				denormMask = denormMask | FValue
			case SumField, SumFieldShort:
				denormMask = denormMask | FSum
			default:
				return 0, fmt.Errorf("unexpected senml field: %s", field)

			}
		}
	}
	return denormMask, nil
}

func parseFromValue(from string) (time.Time, error) {
	if from == "" {
		// start from zero time value
		return time.Time{}, nil
	} else {
		return time.Parse(time.RFC3339, from)
	}
}
func parseToValue(from string) (time.Time, error) {
	if from == "" {
		// start from zero time value
		return time.Now().UTC(), nil
	} else {
		return time.Parse(time.RFC3339, from)
	}
}
