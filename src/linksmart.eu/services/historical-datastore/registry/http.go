package registry

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

const (
	FTypeOne  = "one"
	FTypeMany = "many"

	MaxPerPage = 100
)

var (
	ErrNotFound = errors.New("Not Found")
	ErrConflict = errors.New("Conflict")
)

func ErrType(err, e error) bool {
	return strings.Contains(err.Error(), e.Error())
}

// Read-only HTTP Registry API
type ReadableAPI struct {
	storage Storage
}

// Returns the configured read-only Registry API
func NewReadableAPI(storage Storage) *ReadableAPI {
	return &ReadableAPI{
		storage,
	}
}

// Full HTTP Registry API
type WriteableAPI struct {
	*ReadableAPI
}

// Returns the configured full Registry API
func NewWriteableAPI(storage Storage) *WriteableAPI {
	return &WriteableAPI{
		NewReadableAPI(storage),
	}
}

// Handlers ///////////////////////////////////////////////////////////////////////

// Index is a handler for the registry index
func (regAPI *ReadableAPI) Index(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	lastModified, err := regAPI.storage.modifiedDate()
	if err != nil {
		log.Println("Error retrieving last modified date: %s", err.Error())
		lastModified = time.Now()
	}

	if r.Header.Get("If-Modified-Since") != "" {
		modifiedSince, err := time.Parse(time.RFC1123, r.Header.Get("If-Modified-Since"))
		if err != nil {
			common.ErrorResponse(http.StatusBadRequest, "Error parsing If-Modified-Since header: "+err.Error(), w)
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
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	datasources, total, err := regAPI.storage.getMany(page, perPage)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}

	// Create a registry catalog
	registry := Registry{
		URL:     common.RegistryAPILoc,
		Entries: datasources,
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

// Create is a handler for creating a new DataSource: not supported by Readable API
func (regAPI *ReadableAPI) Create(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// Create is a handler for creating a new DataSource
func (regAPI *WriteableAPI) Create(w http.ResponseWriter, r *http.Request) {

	//	contentType := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	//	if contentType != "application/json" {
	//		common.ErrorResponse(http.StatusUnsupportedMediaType, "Unsupported content type: "+contentType, w)
	//		return
	//	}

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	addedDS, err := regAPI.storage.add(ds)
	if err != nil {
		if ErrType(err, ErrConflict) {
			common.ErrorResponse(http.StatusConflict, err.Error(), w)
		} else {
			common.ErrorResponse(http.StatusInternalServerError, "Error storing data source: "+err.Error(), w)
		}
		return
	}

	//b, _ := json.Marshal(&addedDS)
	w.Header().Set("Location", addedDS.URL)
	//w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusCreated)
	//w.Write(b)

	return
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func (regAPI *ReadableAPI) Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	ds, err := regAPI.storage.get(id)
	if err != nil {
		if ErrType(err, ErrNotFound) {
			common.ErrorResponse(http.StatusNotFound, err.Error(), w)
		} else {
			common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data source: "+err.Error(), w)
		}
		return
	}

	b, _ := json.Marshal(&ds)

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(b)

	return
}

// Update is a handler for updating the given DataSource: not supported by Readable API
func (regAPI *ReadableAPI) Update(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// Update is a handler for updating the given DataSource
// Expected parameters: id
func (regAPI *WriteableAPI) Update(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	_, err = regAPI.storage.update(id, ds)
	if err != nil {
		if ErrType(err, ErrConflict) {
			common.ErrorResponse(http.StatusConflict, err.Error(), w)
		} else if ErrType(err, ErrNotFound) {
			common.ErrorResponse(http.StatusNotFound, err.Error(), w)
		} else {
			common.ErrorResponse(http.StatusInternalServerError, "Error updating data source: "+err.Error(), w)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

// Delete is a handler for deleting the given DataSource: not supported by Readable API
func (regAPI *ReadableAPI) Delete(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// Delete is a handler for deleting the given DataSource
// Expected parameters: id
func (regAPI *WriteableAPI) Delete(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	err := regAPI.storage.delete(id)
	if err != nil {
		if ErrType(err, ErrNotFound) {
			common.ErrorResponse(http.StatusNotFound, err.Error(), w)
		} else {
			common.ErrorResponse(http.StatusInternalServerError, "Error deleting data source: "+err.Error(), w)
		}
		return
	}

	return
}

// Filter is a handler for registry filtering API
// Expected parameters: path, type, op, value
func (regAPI *ReadableAPI) Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	ftype := params["type"]
	fop := params["op"]
	fvalue := params["value"]

	r.ParseForm()
	page, perPage, err := common.ParsePagingParams(r.Form.Get(common.ParamPage), r.Form.Get(common.ParamPerPage), MaxPerPage)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	var body []byte
	switch ftype {
	case FTypeOne:
		datasource, err := regAPI.storage.pathFilterOne(fpath, fop, fvalue)
		if err != nil {
			common.ErrorResponse(http.StatusBadRequest, "Error processing the request: "+err.Error(), w)
			return
		}

		if datasource.ID != "" {
			body, _ = json.Marshal(&datasource)
		} else {
			common.ErrorResponse(http.StatusNotFound, "No matched entries found.", w)
			return
		}

	case FTypeMany:
		datasources, total, err := regAPI.storage.pathFilter(fpath, fop, fvalue, page, perPage)
		if err != nil {
			common.ErrorResponse(http.StatusBadRequest, "Error processing the request: "+err.Error(), w)
			return
		}

		// Create a registry catalog
		registry := Registry{
			URL:     common.RegistryAPILoc,
			Entries: datasources,
			Page:    page,
			PerPage: perPage,
			Total:   total,
		}

		if registry.Total != 0 {
			body, _ = json.Marshal(&registry)
		} else {
			common.ErrorResponse(http.StatusNotFound, "No matched entries found.", w)
			return
		}
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(body)
}
