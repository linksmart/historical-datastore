// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/cisco/senml"
	"github.com/gorilla/mux"
)

const (
	MaxPerPage = 1000
)

// HTTPAPI describes the RESTful HTTP data API
type HTTPAPI struct {
	registryClient   registry.Client
	storage          Storage
	autoRegistration bool
}

// NewHTTPAPI returns the configured Data API
func NewHTTPAPI(registryClient registry.Client, storage Storage, autoRegistration bool) *HTTPAPI {
	logger.Printf("Automatic registration: %v", autoRegistration)
	return &HTTPAPI{registryClient, storage, autoRegistration}
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
// TODO: check SupportedContentTypes instead of hard-coding SenML
func (d *HTTPAPI) Submit(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	data := make(map[string][]senml.SenMLRecord)
	sources := make(map[string]*registry.DataSource)

	//contentType := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	//// Only SenML is supported for now
	//if contentType != "application/senml+json" {
	//	common.ErrorResponse(http.StatusUnsupportedMediaType, "Unsupported content type: "+contentType+". Currently, only `application/senml+json` is supported.", w)
	//	return
	//}

	// Parse id(s)
	ids := strings.Split(params["id"], common.IDSeparator)

	// Read body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Parse payload
	senmlPack, err := senml.Decode(body, senml.JSON)
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

	// Fill the data map with provided data points
	records := senml.Normalize(senmlPack).Records
	for _, r := range records {
		if r.Name == "" {
			common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Data source name not specified."), w)
			return
		}
		// Check if there is a data source for this entry
		ds, ok := dsResources[r.Name]
		if !ok {
			common.ErrorResponse(http.StatusNotFound, fmt.Sprintf("Data point for unknown data source %v.", r.Name), w)
			return
		}

		// Check if type of value matches the data source type in registry
		typeError := false
		switch ds.Type {
		case common.FLOAT:
			if r.Value == nil {
				typeError = true
			}
		case common.STRING:
			if r.StringValue == "" {
				typeError = true
			}
		case common.BOOL:
			if r.BoolValue == nil {
				typeError = true
			}
		}
		if typeError {
			common.ErrorResponse(http.StatusBadRequest,
				fmt.Sprintf("Value for %v is empty or has a type other than what is set in registry: %v", r.Name, ds.Type), w)
			return
		}

		_, ok = data[ds.ID]
		if !ok {
			data[ds.ID] = []senml.SenMLRecord{}
			sources[ds.ID] = ds
		}
		data[ds.ID] = append(data[ds.ID], r)
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

	// Read body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Parse payload
	senmlPack, err := senml.Decode(body, senml.JSON)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	// map of resource name -> data source
	nameDSs := make(map[string]*registry.DataSource)

	// Fill the data map with provided data points
	data := make(map[string][]senml.SenMLRecord)
	sources := make(map[string]*registry.DataSource)
	records := senml.Normalize(senmlPack).Records
	for _, r := range records {

		ds, found := nameDSs[r.Name]
		if !found {
			ds, err = d.registryClient.FindDataSource("resource", "equals", r.Name)
			if err != nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Error retrieving data source with name %v from the registry: %v", r.Name, err.Error()), w)
				return
			}
			if ds == nil {
				if !d.autoRegistration {
					common.ErrorResponse(http.StatusNotFound, fmt.Sprintf("Data source with name %v is not registered.", r.Name), w)
					return
				}

				// Register a data source with this name
				logger.Printf("Registering data source for %s", r.Name)
				newDS := registry.DataSource{
					Resource: r.Name,
					Format:   "application/senml+json",
					Meta: map[string]interface{}{
						"registrar": "Data API",
					},
				}
				if r.Value != nil {
					newDS.Type = common.FLOAT
				} else if r.StringValue != "" {
					newDS.Type = common.STRING
				} else {
					newDS.Type = common.BOOL
				}
				addedDS, err := d.registryClient.Add(newDS)
				if err != nil {
					common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Error registering %v in the registry: %v", r.Name, err.Error()), w)
					return
				}
				ds = &addedDS
			}
			nameDSs[r.Name] = ds
		}

		// Check if type of value matches the data source type in registry
		typeError := false
		switch ds.Type {
		case common.FLOAT:
			if r.Value == nil {
				typeError = true
			}
		case common.STRING:
			if r.StringValue == "" {
				typeError = true
			}
		case common.BOOL:
			if r.BoolValue == nil {
				typeError = true
			}
		}
		if typeError {
			common.ErrorResponse(http.StatusBadRequest,
				fmt.Sprintf("Value for %v is empty or has a type other than what is set in registry: %v", r.Name, ds.Type), w)
			return
		}

		// Prepare for storage
		_, found = data[ds.ID]
		if !found {
			data[ds.ID] = []senml.SenMLRecord{}
			sources[ds.ID] = ds
		}
		data[ds.ID] = append(data[ds.ID], r)
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
		Time:    time.Since(timeStart).Seconds() * 1000,
		Data:    data.Records,
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
