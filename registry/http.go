// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/linksmart/historical-datastore/common"
)

const (
	FTypeOne  = "one"
	FTypeMany = "many"

	MaxPerPage = 100
)

var (
	ErrNotFound   = &common.NotFoundError{S: "time series not found"}
	ErrConflict   = &common.ConflictError{S: "conflict"}
	ErrBadRequest = &common.BadRequestError{S: "invald time series"}
)

// RESTful HTTP API
type API struct {
	storage Storage
}

// Returns the configured TimeSeriesList API
func NewAPI(storage Storage) *API {
	return &API{
		storage,
	}
}

// Handlers ///////////////////////////////////////////////////////////////////////

// Index is a handler for the registry index
func (api *API) Index(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	//TODO: add nextLink
	lastModified, err := api.storage.getLastModifiedTime()
	if err != nil {
		log.Printf("Error retrieving last modified date: %v", err)
		lastModified = time.Now()
	}

	if r.Header.Get("If-Modified-Since") != "" {
		modifiedSince, err := time.Parse(time.RFC1123, r.Header.Get("If-Modified-Since"))
		if err != nil {
			common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing If-Modified-Since header: " + err.Error()}, w)
			return
		}
		lastModified, _ = time.Parse(time.RFC1123, lastModified.UTC().Format(time.RFC1123))
		if modifiedSince.Equal(lastModified) || modifiedSince.After(lastModified) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	page, perPage, err := common.ParsePagingParams(r.Form.Get(common.ParamPage), r.Form.Get(common.ParamPerPage), MaxPerPage)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	series, total, err := api.storage.GetMany(page, perPage)
	if err != nil {
		common.HttpErrorResponse(&common.InternalError{S: err.Error()}, w)
		return
	}

	// Create a registry catalog
	registry := TimeSeriesList{
		Series:  series,
		Page:    page,
		PerPage: perPage,
		Total:   total,
	}

	registry.DataLink = dataLinkFromRegistryList(registry.Series)

	b, _ := json.Marshal(&registry)
	w.Header().Add("Content-Type", common.DefaultMIMEType)
	w.Header().Add("Last-Modified", lastModified.UTC().Format(time.RFC1123))
	w.Write(b)
	return
}

// Create is a handler for creating a new DataSource
func (api *API) Create(w http.ResponseWriter, r *http.Request) {

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	var ts TimeSeries
	err = json.Unmarshal(body, &ts)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error processing input: " + err.Error()}, w)
		return
	}

	addedTS, err := api.storage.Add(ts)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			common.HttpErrorResponse(&common.ConflictError{S: err.Error()}, w)
		} else if errors.Is(err, ErrBadRequest) {
			common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error storing time series: " + err.Error()}, w)
		}
		return
	}

	//b, _ := json.Marshal(&addedTS)
	w.Header().Set("Location", common.RegistryAPILoc+"/"+addedTS.Name)

	w.WriteHeader(http.StatusCreated)
	//w.Write(b)

	return
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func (api *API) Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	ts, err := api.storage.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			common.HttpErrorResponse(&common.NotFoundError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error retrieving time series: " + err.Error()}, w)
		}
		return
	}

	b, _ := json.Marshal(&ts)

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(b)

	return
}

// Update is a handler for updating the given DataSource
// Expected parameters: id
func (api *API) Update(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	var ts TimeSeries
	err = json.Unmarshal(body, &ts)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error processing input: " + err.Error()}, w)
		return
	}

	_, err = api.storage.Update(id, ts)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			common.HttpErrorResponse(&common.ConflictError{S: err.Error()}, w)
		} else if errors.Is(err, ErrNotFound) {
			common.HttpErrorResponse(&common.NotFoundError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error updating time series: " + err.Error()}, w)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

// Delete is a handler for deleting the given DataSource
// Expected parameters: id
func (api *API) Delete(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	err := api.storage.Delete(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			common.HttpErrorResponse(&common.NotFoundError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error deleting time series: " + err.Error()}, w)
		}
		return
	}

	return
}

// Filter is a handler for registry filtering API
// Expected parameters: path, type, op, value
func (api *API) Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	ftype := params["type"]
	fop := params["op"]
	fvalue := params["value"]

	r.ParseForm()
	page, perPage, err := common.ParsePagingParams(r.Form.Get(common.ParamPage), r.Form.Get(common.ParamPerPage), MaxPerPage)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing pagination parameters:" + err.Error()}, w)
		return
	}

	var body []byte
	switch ftype {
	case FTypeOne:
		timeSeries, err := api.storage.FilterOne(fpath, fop, fvalue)
		if err != nil {
			common.HttpErrorResponse(&common.InternalError{S: "Error processing the filter request:" + err.Error()}, w)
			return
		}
		// Respond with a catalog
		registry := TimeSeriesList{
			Series:  []TimeSeries{},
			Page:    page,
			PerPage: perPage,
			Total:   0,
		}
		if timeSeries != nil {
			registry.Series = append(registry.Series, *timeSeries)
			registry.Total++
		}
		registry.DataLink = dataLinkFromRegistryList(registry.Series)
		body, _ = json.Marshal(&registry)

	case FTypeMany:
		timeSeries, total, err := api.storage.Filter(fpath, fop, fvalue, page, perPage)
		if err != nil {
			common.HttpErrorResponse(&common.InternalError{S: "Error processing the filter request:" + err.Error()}, w)
			return
		}

		// Respond with a catalog
		registry := TimeSeriesList{
			Series:  timeSeries,
			Page:    page,
			PerPage: perPage,
			Total:   total,
		}
		registry.DataLink = dataLinkFromRegistryList(registry.Series)
		body, _ = json.Marshal(&registry)
	default:
		common.HttpErrorResponse(&common.BadRequestError{S: fmt.Sprintf("Invalid filter command %s: expected: '%s' or '%s'", ftype, FTypeMany, FTypeOne)}, w)
		return
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(body)
}

func dataLinkFromRegistryList(seriesList []TimeSeries) string {
	var linkBuilder strings.Builder
	separator := common.DataAPILoc + "/"
	for _, series := range seriesList {
		linkBuilder.WriteString(separator + series.Name)
		separator = ","
	}
	return linkBuilder.String()
}
