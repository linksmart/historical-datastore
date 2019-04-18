// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/farshidtz/senml"
	"github.com/gorilla/mux"
)

const (
	MaxPerPage = 1000
)

// API describes the RESTful HTTP data API
type API struct {
	registry         registry.Storage
	storage          Storage
	autoRegistration bool
}

// NewAPI returns the configured Data API
func NewAPI(registry registry.Storage, storage Storage, autoRegistration bool) *API {
	return &API{registry, storage, autoRegistration}
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
func (api *API) Submit(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	data := make(map[string]senml.Pack)
	sources := make(map[string]*registry.DataStream)

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

	// Check if DataSources are registered in the DataStreamList
	dsResources := make(map[string]*registry.DataStream)
	for _, id := range ids {
		ds, err := api.registry.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving data source %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		dsResources[ds.Name] = ds
	}

	// Fill the data map with provided data points
	records := senmlPack.Normalize()
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

		_, ok = data[ds.Name]
		if !ok {
			data[ds.Name] = senml.Pack{}
			sources[ds.Name] = ds
		}
		data[ds.Name] = append(data[ds.Name], r)
	}

	// Add data to the storage
	err = api.storage.Submit(data, sources)
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
func (api *API) SubmitWithoutID(w http.ResponseWriter, r *http.Request) {

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
	nameDSs := make(map[string]*registry.DataStream)

	// Fill the data map with provided data points
	data := make(map[string]senml.Pack)
	sources := make(map[string]*registry.DataStream)
	records := senmlPack.Normalize()
	for _, r := range records {

		ds, found := nameDSs[r.Name]
		if !found {
			ds, err = api.registry.FilterOne("resource", "equals", r.Name)
			if err != nil {
				common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Error retrieving data source with name %v from the registry: %v", r.Name, err.Error()), w)
				return
			}
			if ds == nil {
				if !api.autoRegistration {
					common.ErrorResponse(http.StatusNotFound, fmt.Sprintf("Data source with name %v is not registered.", r.Name), w)
					return
				}

				// Register a data source with this name
				log.Printf("Registering data source for %s", r.Name)
				newDS := registry.DataStream{
					Name: r.Name,
				}
				if r.Value != nil || r.Sum != nil {
					newDS.Type = common.FLOAT
				} else if r.StringValue != "" {
					newDS.Type = common.STRING
				} else if r.BoolValue != nil {
					newDS.Type = common.BOOL
				} else if r.DataValue != "" {
					newDS.Type = common.DATA
				}
				addedDS, err := api.registry.Add(newDS)
				if err != nil {
					common.ErrorResponse(http.StatusBadRequest, fmt.Sprintf("Error registering %v in the registry: %v", r.Name, err.Error()), w)
					return
				}
				ds = addedDS
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
		_, found = data[ds.Name]
		if !found {
			data[ds.Name] = senml.Pack{}
			sources[ds.Name] = ds
		}
		data[ds.Name] = append(data[ds.Name], r)
	}

	// Add data to the storage
	err = api.storage.Submit(data, sources)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error writing data to the database: "+err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusAccepted)
	return
}

func GetUrlFromQuery(q Query, id ...string) (url string) {
	var sort, limit, start, end, perPage string
	if q.Sort != "" {
		sort = fmt.Sprintf("&%v=%v", common.ParamSort, q.Sort)
	}
	if q.Limit > 0 {
		limit = fmt.Sprintf("&%v=%v", common.ParamLimit, q.Limit)
	}
	if !q.From.IsZero() {
		start = fmt.Sprintf("&%v=%v", common.ParamFrom, q.From.UTC().Format(time.RFC3339))
	}
	if !q.To.IsZero() {
		end = fmt.Sprintf("&%v=%v", common.ParamTo, q.To.UTC().Format(time.RFC3339))
	}

	if q.perPage > 0 {
		perPage = fmt.Sprintf("&%v=%v", common.ParamPerPage, q.perPage)
	}

	return fmt.Sprintf("%v?%s%s%s%s%s",
		strings.Join(id, common.IDSeparator),
		perPage,
		sort, limit, start, end,
	)
}

// Query is a handler for querying data
// Expected parameters: id(s), optional: pagination, query string
func (api *API) Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	timeStart := time.Now()
	params := mux.Vars(r)
	var recordSet RecordSet

	// Parse id(s) and get sources from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	sources := []*registry.DataStream{}
	for _, id := range ids {
		ds, err := api.registry.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving data source %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		sources = append(sources, ds)
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

	data, total, nextLinkTS, err := api.storage.Query(q, sources...)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
		return
	}

	curlink := common.DataAPILoc + "/" + GetUrlFromQuery(q, ids...)

	nextlink := ""

	if nextLinkTS != nil {
		nextQuery := q
		lastPage := false
		if q.Limit > 0 { //if Limit is given by user reduce the limit by total
			newLimit := q.Limit - total
			if newLimit > 0 {
				nextQuery.Limit = newLimit
			} else {
				lastPage = true
			}
		}

		if !lastPage {
			if q.Sort == common.DESC {
				nextQuery.To = *nextLinkTS
			} else {
				nextQuery.From = *nextLinkTS
			}
			nextlink = common.DataAPILoc + "/" + GetUrlFromQuery(nextQuery, ids...)
		}
	}

	recordSet = RecordSet{
		SelfLink: curlink,
		TimeTook: time.Since(timeStart).Seconds(),
		Data:     data,
		NextLink: nextlink,
	}

	csvStr, err := json.Marshal(recordSet)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error marshalling recordset: "+err.Error(), w)
		return
	}

	w.Header().Add("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(csvStr))
}

// Utility functions

func ParseQueryParameters(form url.Values) (Query, error) {
	q := Query{}
	var err error

	// start time
	if form.Get(common.ParamFrom) == "" {
		// Start from zero time
		q.From = time.Time{}
	} else {
		q.From, err = time.Parse(time.RFC3339, form.Get(common.ParamFrom))
		if err != nil {
			return Query{}, fmt.Errorf("Error parsing start argument: %s", err)
		}
	}

	// end time
	if form.Get(common.ParamTo) == "" {
		// Open-ended query
		q.To = time.Now().UTC()
	} else {
		q.To, err = time.Parse(time.RFC3339, form.Get(common.ParamTo))
		if err != nil {
			return Query{}, fmt.Errorf("Error parsing end argument: %s", err)
		}
	}

	if !q.To.After(q.From) {
		return Query{}, fmt.Errorf("end argument is before or equal to start")
	}

	// limit
	if form.Get(common.ParamLimit) == "" {
		q.Limit = -1
	} else {
		q.Limit, err = strconv.Atoi(form.Get(common.ParamLimit))
		if err != nil {
			return Query{}, fmt.Errorf("Error parsing limit argument: %s", err)
		}
	}

	// sort
	q.Sort = form.Get(common.ParamSort)
	if q.Sort == "" {
		// default sorting order
		q.Sort = common.DESC
	} else if q.Sort != common.ASC && q.Sort != common.DESC {
		return Query{}, fmt.Errorf("Invalid sort argument: %v", q.Sort)
	}

	if form.Get(common.ParamPerPage) == "" {
		q.perPage = MaxPerPage
	} else {
		q.perPage, err = strconv.Atoi(form.Get(common.ParamPerPage))
		if err != nil {
			return Query{}, fmt.Errorf("Error parsing limit argument: %s", err)
		}
	}

	return q, nil
}
