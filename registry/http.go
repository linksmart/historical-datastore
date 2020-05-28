// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
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
	ErrNotFound   = &common.NotFoundError{S: "datastream not found"}
	ErrConflict   = &common.ConflictError{S: "conflict"}
	ErrBadRequest = &common.BadRequestError{S: "invald datastream"}
)

// RESTful HTTP API
type API struct {
	storage Storage
}

// Returns the configured DataStreamList API
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

	dataStreams, total, err := api.storage.GetMany(page, perPage)
	if err != nil {
		common.HttpErrorResponse(&common.InternalError{S: err.Error()}, w)
		return
	}

	// Create a registry catalog
	registry := DataStreamList{
		URL:     common.RegistryAPILoc,
		Streams: dataStreams,
		Page:    page,
		PerPage: perPage,
		Total:   total,
	}

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

	var ds DataStream
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error processing input: " + err.Error()}, w)
		return
	}

	addedDS, err := api.storage.Add(ds)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			common.HttpErrorResponse(&common.ConflictError{S: err.Error()}, w)
		} else if errors.Is(err, ErrBadRequest) {
			common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error storing Data stream: " + err.Error()}, w)
		}
		return
	}

	//b, _ := json.Marshal(&addedDS)
	w.Header().Set("Location", common.RegistryAPILoc+"/"+addedDS.Name)
	//w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusCreated)
	//w.Write(b)

	return
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func (api *API) Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	ds, err := api.storage.Get(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			common.HttpErrorResponse(&common.NotFoundError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error retrieving Data stream: " + err.Error()}, w)
		}
		return
	}

	b, _ := json.Marshal(&ds)

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

	var ds DataStream
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error processing input: " + err.Error()}, w)
		return
	}

	_, err = api.storage.Update(id, ds)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			common.HttpErrorResponse(&common.ConflictError{S: err.Error()}, w)
		} else if errors.Is(err, ErrNotFound) {
			common.HttpErrorResponse(&common.NotFoundError{S: err.Error()}, w)
		} else {
			common.HttpErrorResponse(&common.InternalError{S: "Error updating Data stream: " + err.Error()}, w)
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
			common.HttpErrorResponse(&common.InternalError{S: "Error deleting Data stream: " + err.Error()}, w)
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
		dataStream, err := api.storage.FilterOne(fpath, fop, fvalue)
		if err != nil {
			common.HttpErrorResponse(&common.InternalError{S: "Error processing the filter request:" + err.Error()}, w)
			return
		}

		// Respond with a catalog
		registry := DataStreamList{
			URL:     common.RegistryAPILoc,
			Streams: []DataStream{},
			Page:    page,
			PerPage: perPage,
			Total:   0,
		}
		if dataStream != nil {
			registry.Streams = append(registry.Streams, *dataStream)
			registry.Total++
		}
		body, _ = json.Marshal(&registry)

	case FTypeMany:
		dataStreams, total, err := api.storage.Filter(fpath, fop, fvalue, page, perPage)
		if err != nil {
			common.HttpErrorResponse(&common.InternalError{S: "Error processing the filter request:" + err.Error()}, w)
			return
		}

		// Respond with a catalog
		registry := DataStreamList{
			URL:     common.RegistryAPILoc,
			Streams: dataStreams,
			Page:    page,
			PerPage: perPage,
			Total:   total,
		}
		body, _ = json.Marshal(&registry)
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(body)
}
