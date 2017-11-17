// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/gorilla/mux"
	senml "github.com/krylovsk/gosenml"
)

const (
	MaxPerPage = 1000
)

// HTTPAPI describes the RESTful HTTP data API
type HTTPAPI struct {
	registryClient registry.Client
	storage        Storage
}

// NewHTTPAPI returns the configured Data API
func NewHTTPAPI(registryClient registry.Client, storage Storage) *HTTPAPI {
	return &HTTPAPI{
		registryClient,
		storage,
	}
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
// TODO: check SupportedContentTypes instead of hard-coding SenML
func (d *HTTPAPI) Submit(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	data := make(map[string][]DataPoint)
	sources := make(map[string]*registry.DataSource)

	//contentType := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	//// Only SenML is supported for now
	//if contentType != "application/senml+json" {
	//	common.ErrorResponse(http.StatusUnsupportedMediaType, "Unsupported content type: "+contentType+". Currently, only `application/senml+json` is supported.", w)
	//	return
	//}

	// Parse id(s)
	ids := strings.Split(params["id"], common.IDSeparator)

	// Parse payload
	var senmlMessage senml.Message
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	err := decoder.Decode(&senmlMessage)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	// Check if DataSources are registered in the Registry
	dsResources := make(map[string]*registry.DataSource)
	for _, id := range ids {
		ds, err := d.registryClient.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving data source %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		dsResources[ds.Resource] = &ds
	}

	err = senmlMessage.Validate()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Fill the data map with provided data points
	entries := senmlMessage.Expand().Entries
	for _, e := range entries {
		if e.Name == "" {
			common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Data source name not specified."), w)
			return
		}
		// Check if there is a data source for this entry
		ds, ok := dsResources[e.Name]
		if !ok {
			common.ErrorResponse(http.StatusNotFound, fmt.Sprintf("Data point for unknown data source %v.", e.Name), w)
			return
		}

		// Check if type of value matches the data source type in registry
		typeError := false
		switch ds.Type {
		case common.FLOAT:
			if e.BooleanValue != nil || e.StringValue != nil && *e.StringValue != "" {
				typeError = true
			}
		case common.STRING:
			if e.Value != nil || e.BooleanValue != nil {
				typeError = true
			}
		case common.BOOL:
			if e.Value != nil || e.StringValue != nil && *e.StringValue != "" {
				typeError = true
			}
		}
		if typeError {
			common.ErrorResponse(http.StatusBadRequest,
				fmt.Sprintf("Entry for data point %v has a type that is incompatible with source registration. Source %v has type %v.", e.Name, ds.ID, ds.Type), w)
			return
		}

		_, ok = data[ds.ID]
		if !ok {
			data[ds.ID] = []DataPoint{}
			sources[ds.ID] = ds
		}
		p := NewDataPoint()
		data[ds.ID] = append(data[ds.ID], p.FromEntry(e))
	}

	// Add data to the storage
	err = d.storage.Submit(data, sources)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error writing data to the database: "+err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusAccepted)
	return
}

// SubmitWithoutID is a handler for submitting a new data point
// Expected parameters: none
func (d *HTTPAPI) SubmitWithoutID(w http.ResponseWriter, r *http.Request) {

	// Parse payload
	var senmlMessage senml.Message
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	err := decoder.Decode(&senmlMessage)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	// map of resource name -> data source
	nameDSs := make(map[string]*registry.DataSource)

	err = senmlMessage.Validate()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Fill the data map with provided data points
	data := make(map[string][]DataPoint)
	sources := make(map[string]*registry.DataSource)
	entries := senmlMessage.Expand().Entries
	for _, e := range entries {
		if e.Name == "" {
			common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("SenML name not specified."), w)
			return
		}

		ds, found := nameDSs[e.Name]
		if !found {
			ds, err = d.registryClient.FindDataSource("name", "equals", e.Name)
			if err != nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Error retrieving data source with name %v from the registry: %v", e.Name, err.Error()), w)
				return
			}
			if ds == nil {
				common.ErrorResponse(http.StatusNotFound, fmt.Sprintf("Data source with name %v is not registered.", e.Name), w)
				return
			}
			nameDSs[e.Name] = ds
		}

		// Check if type of value matches the data source type in registry
		switch ds.Type {
		case common.FLOAT:
			if e.Value == nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Entry %s has type float that mismatches the registration type %s.", e.Name, ds.Type), w)
				return
			}
		case common.STRING:
			if e.StringValue == nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Entry %s has type string that mismatches the registration type %s.", e.Name, ds.Type), w)
				return
			}
		case common.BOOL:
			if e.BooleanValue == nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Entry %s has type boolean that mismatches the registration type %s.", e.Name, ds.Type), w)
				return
			}
		}

		// Prepare for storage
		_, found = data[ds.ID]
		if !found {
			data[ds.ID] = []DataPoint{}
			sources[ds.ID] = ds
		}
		p := NewDataPoint()
		data[ds.ID] = append(data[ds.ID], p.FromEntry(e))
	}

	// Add data to the storage
	err = d.storage.Submit(data, sources)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error writing data to the database: "+err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusAccepted)
	return
}

// Query is a handler for querying data
// Expected parameters: id(s), optional: pagination, query string
func (d *HTTPAPI) Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	timeStart := time.Now()
	params := mux.Vars(r)
	var (
		page, perPage int
		recordSet     RecordSet
	)

	page, perPage, err := common.ParsePagingParams(r.Form.Get(common.ParamPage), r.Form.Get(common.ParamPerPage), MaxPerPage)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Parse id(s) and get sources from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	sources := []*registry.DataSource{}
	for _, id := range ids {
		ds, err := d.registryClient.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving data source %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		sources = append(sources, &ds)
	}
	if len(sources) == 0 {
		common.ErrorResponse(http.StatusNotFound,
			"None of the specified data sources could be retrieved from the registry.", w)
		return
	}

	// Parse query
	q, err := ParseQueryParameters(r.Form)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	err = common.ValidatePerItemLimit(q.Limit, perPage, len(sources))
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	data, total, err := d.storage.Query(q, page, perPage, sources...)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
		return
	}

	v := url.Values{}
	v.Add(common.ParamStart, q.Start.Format(time.RFC3339))
	// Omit end in open-ended queries
	if q.End.After(q.Start) {
		v.Add(common.ParamEnd, q.End.Format(time.RFC3339))
	}
	v.Add(common.ParamSort, q.Sort)
	if q.Limit > 0 { // non-positive limit is ignored
		v.Add(common.ParamLimit, fmt.Sprintf("%d", q.Limit))
	}
	v.Add(common.ParamPage, fmt.Sprintf("%d", page))
	v.Add(common.ParamPerPage, fmt.Sprintf("%d", perPage))
	recordSet = RecordSet{
		URL:     fmt.Sprintf("%s?%s", r.URL.Path, v.Encode()),
		Data:    data,
		Time:    time.Since(timeStart).Seconds() * 1000,
		Page:    page,
		PerPage: perPage,
		Total:   total,
	}

	b, err := json.Marshal(recordSet)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error marshalling recordset: "+err.Error(), w)
		return
	}

	w.Header().Add("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

// Utility functions

func ParseQueryParameters(form url.Values) (Query, error) {
	q := Query{}
	var err error

	// start time
	if form.Get(common.ParamStart) == "" {
		// Start from zero time
		q.Start = time.Time{}
	} else {
		q.Start, err = time.Parse(time.RFC3339, form.Get(common.ParamStart))
		if err != nil {
			return Query{}, logger.Errorf("Error parsing start argument: %s", err)
		}
	}

	// end time
	if form.Get(common.ParamEnd) == "" {
		// Open-ended query
		q.End = time.Now().UTC()
	} else {
		q.End, err = time.Parse(time.RFC3339, form.Get(common.ParamEnd))
		if err != nil {
			return Query{}, logger.Errorf("Error parsing end argument: %s", err)
		}
	}

	if !q.End.After(q.Start) {
		return Query{}, logger.Errorf("end argument is before or equal to start")
	}

	// limit
	if form.Get(common.ParamLimit) == "" {
		q.Limit = -1
	} else {
		q.Limit, err = strconv.Atoi(form.Get(common.ParamLimit))
		if err != nil {
			return Query{}, logger.Errorf("Error parsing limit argument: %s", err)
		}
	}

	// sort
	q.Sort = form.Get(common.ParamSort)
	if q.Sort == "" {
		// default sorting order
		q.Sort = common.DESC
	} else if q.Sort != common.ASC && q.Sort != common.DESC {
		return Query{}, logger.Errorf("Invalid sort argument: %v", q.Sort)
	}

	return q, nil
}
