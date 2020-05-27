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
	streams := make(map[string]*registry.DataStream)
	nameDSs := make(map[string]*registry.DataStream)
	fromStreamList := false
	if ids != nil {
		for _, id := range ids {
			ds, err := c.registry.Get(id)
			if err != nil {
				return &common.NotFoundError{S: fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", id, err)}
			}
			streams[ds.Name] = ds
		}
		fromStreamList = true
	}

	// Fill the data map with provided data points
	data := make(map[string]senml.Pack)
	senmlPack.Normalize()
	for _, r := range senmlPack {
		ds, found := nameDSs[r.Name]
		if !found && fromStreamList {
			return &common.BadRequestError{S: fmt.Sprintf("senml entry %s does not match the provided datastream", r.Name)}
		}
		if !found {
			ds, err := c.registry.Get(r.Name)
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
				}
				nameDSs[r.Name] = ds
			} else {
				return &common.InternalError{S: err.Error()}
			}
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

func (c Controller) Query(q Query, ids []string) (pack senml.Pack, total *int, retErr common.Error) {
	var sources []*registry.DataStream
	for _, id := range ids {
		ds, err := c.registry.Get(id)
		if err != nil {
			return nil, nil, &common.InternalError{S: fmt.Sprintf("Error retrieving Data stream %v from the registry: %w", id, err)}
		}
		sources = append(sources, ds)
	}

	if len(sources) == 0 {
		return nil, nil, &common.NotFoundError{S: "None of the specified Data streams could be retrieved from the registry."}
	}

	data, total, err := c.storage.Query(q, sources...)
	if err != nil {
		return nil, nil, &common.InternalError{S: "Error retrieving data from the database: " + err.Error()}
		return
	}
	return data, total, nil
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
