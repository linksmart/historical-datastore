package data

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cskr/pubsub"
	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	errors2 "github.com/syndtr/goleveldb/leveldb/errors"
)

type Controller struct {
	registry         registry.Controller
	storage          Storage
	autoRegistration bool
	pubSub           *pubsub.PubSub
}

// NewAPI returns the configured Data API
func NewController(registry registry.Controller, storage Storage, autoRegistration bool) *Controller {
	pubSubClient := pubsub.New(0)
	return &Controller{registry: registry, storage: storage, autoRegistration: autoRegistration, pubSub: pubSubClient}
}

//TODO: Return right code in return so that right code is returned by callers. e.g. Grpc code or http error responses.
func (c Controller) Submit(ctx context.Context, senmlPack senml.Pack, ids []string) common.Error {
	const Y3K = 32503680000 //Year 3000 BC, beyond which the time values are not taken
	//series := make(map[string]*registry.TimeSeries)
	nameTS := make(map[string]*registry.TimeSeries)
	fromSeriesList := false
	if ids != nil {
		for _, id := range ids {
			ts, err := c.registry.Get(id)
			if err != nil {
				return err
			}
			nameTS[ts.Name] = ts
		}
		fromSeriesList = true
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
		ts, found := nameTS[r.Name]
		if !found && fromSeriesList {
			return &common.BadRequestError{S: fmt.Sprintf("senml entry %s does not match the provided time series", r.Name)}
		}
		if !found {
			var err error
			ts, err = c.registry.Get(r.Name)
			if err != nil {
				if errors.Is(err, errors2.ErrNotFound) {
					if !c.autoRegistration {
						return &common.NotFoundError{S: fmt.Sprintf("Time series with name %v is not registered.", r.Name)}
					}

					// Register a  time series with this name
					log.Printf("Registering time series for %s", r.Name)
					newTS := registry.TimeSeries{
						Name: r.Name,
						Unit: r.Unit,
					}
					if r.Value != nil || r.Sum != nil {
						newTS.Type = registry.Float
					} else if r.StringValue != "" {
						newTS.Type = registry.String
					} else if r.BoolValue != nil {
						newTS.Type = registry.Bool
					} else if r.DataValue != "" {
						newTS.Type = registry.Data
					}
					addedDS, err := c.registry.Add(newTS)
					if err != nil {
						return &common.BadRequestError{S: fmt.Sprintf("Error registering %v in the registry: %v", r.Name, err)}
					}
					ts = addedDS
				} else {
					return &common.InternalError{S: err.Error()}
				}
			}
			nameTS[r.Name] = ts
		}

		err := validateRecordAgainstRegistry(r, ts)

		if err != nil {
			return &common.BadRequestError{S: fmt.Sprintf("Error validating the record: %v", err)}
		}

		// Prepare for storage
		_, found = data[ts.Name]
		if !found {
			data[ts.Name] = senml.Pack{}
		}
		data[ts.Name] = append(data[ts.Name], r)
	}

	// Add data to the storage
	err := c.storage.Submit(ctx, data, nameTS)
	if err != nil {
		return &common.InternalError{S: "error writing data to the database: " + err.Error()}
	}

	//notify subsribers
	for name, pack := range data {
		c.pubSub.Pub(pack, name)
	}
	return nil
}

func (c Controller) QueryPage(ctx context.Context, q Query, ids []string) (pack senml.Pack, total *int, retErr common.Error) {
	return c.queryStreamOrPage(ctx, q, ids, nil)
}
func (c Controller) QueryStream(ctx context.Context, q Query, ids []string, sendFunc sendFunction) (retErr common.Error) {
	_, _, retErr = c.queryStreamOrPage(ctx, q, ids, sendFunc)
	return retErr
}

func (c Controller) Delete(ctx context.Context, seriesNames []string, from time.Time, to time.Time) (retErr common.Error) {
	var series []*registry.TimeSeries
	for _, seriesName := range seriesNames {
		ts, err := c.registry.Get(seriesName)
		if err != nil {
			return err
		}
		series = append(series, ts)
	}
	if len(series) == 0 {
		return &common.NotFoundError{S: "None of the specified Time series could be retrieved from the registry."}
	}
	err := c.storage.Delete(ctx, series, from, to)
	if err != nil {
		return &common.InternalError{S: "Error deleting the data: " + err.Error()}
	}
	return nil
}

func (c Controller) Count(ctx context.Context, q Query, seriesNames []string) (total int, retErr common.Error) {
	var series []*registry.TimeSeries
	for _, seriesName := range seriesNames {
		ts, err := c.registry.Get(seriesName)
		if err != nil {
			return 0, err
		}
		series = append(series, ts)
	}
	if len(series) == 0 {
		return 0, &common.NotFoundError{S: "None of the specified time series could be retrieved from the registry."}
	}
	total, err := c.storage.Count(ctx, q, series...)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, &common.BadRequestError{S: "timeout trying to prepare a response for the given query"}
		} else {
			return 0, &common.InternalError{S: "Error retrieving count from the database: " + err.Error()}
		}
	}
	return total, nil
}

func (c Controller) queryStreamOrPage(ctx context.Context, q Query, seriesNames []string, sendFunc sendFunction) (pack senml.Pack, total *int, retErr common.Error) {
	var series []*registry.TimeSeries
	for _, seriesName := range seriesNames {
		ts, err := c.registry.Get(seriesName)
		if err != nil {
			return nil, nil, err
		}
		series = append(series, ts)
	}

	if len(series) == 0 {
		return nil, nil, &common.NotFoundError{S: "None of the specified time series could be retrieved from the registry."}
	}

	var err error
	if sendFunc == nil {
		pack, total, err = c.storage.QueryPage(ctx, q, series...)
	} else {
		err = c.storage.QueryStream(ctx, q, sendFunc, series...)
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, &common.BadRequestError{S: "timeout trying to prepare a response for the given query"}
		} else {
			return nil, nil, &common.InternalError{S: "Error retrieving data from the database: " + err.Error()}
		}
	}
	return pack, total, nil
}

func (c Controller) Subscribe(seriesNames ...string) (chan interface{}, common.Error) {
	for _, seriesName := range seriesNames {
		_, err := c.registry.Get(seriesName)
		if err != nil {
			return nil, err
		}
	}
	return c.pubSub.Sub(seriesNames...), nil
}

func (c Controller) Unsubscribe(channel chan interface{}, names ...string) {
	c.pubSub.Unsub(channel, names...)
}
func parseDenormParams(denormStrings []string) (denormMask DenormMask, err error) {

	for _, field := range denormStrings {
		switch strings.ToLower(strings.TrimSpace(field)) {
		case TimeField, TimeFieldShort:
			denormMask = denormMask | DenormMaskTime
		case NameField, NameFieldShort:
			denormMask = denormMask | DenormMaskName
		case UnitField, UnitFieldShort:
			denormMask = denormMask | DenormMaskUnit
		case ValueField, ValueFieldShort:
			denormMask = denormMask | DenormMaskValue
		case SumField, SumFieldShort:
			denormMask = denormMask | DenormMaskSum
		default:
			return 0, fmt.Errorf("unexpected senml field: %s", field)

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

func parseAggregationParams(aggr, window string) (aggrFunction string, duration time.Duration, err error) {
	if aggr == "" && window == "" { // nothing to parse
		return
	} else if aggr == "" || window == "" {
		return "", 0, fmt.Errorf("aggregation function and window size must be set together")
	}

	if !common.SupportedAggregate(aggr) {
		return "", 0, fmt.Errorf("unsupported aggregation function: %s", aggrFunction)
	}

	duration, err = time.ParseDuration(window)
	if err != nil {
		return "", 0, fmt.Errorf("invalid aggregation window: %s", window)
	}
	return aggr, duration, nil
}
