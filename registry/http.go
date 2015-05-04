package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

// Registry api
type RegistryAPI struct {
	storage Storage
}

func NewRegistryAPI(storage Storage) *RegistryAPI {
	return &RegistryAPI{
		storage,
	}
}

const (
	GetParamPage    = "page"
	GetParamPerPage = "per_page"
	// Max DataSources displayed in each page of registry
	MaxPerPage   = 100
	FTypeOne     = "one"
	FTypeMany    = "many"
	httpNotFound = 404
)

// Handlers ///////////////////////////////////////////////////////////////////////

// Index is a handler for the registry index
func (regAPI *RegistryAPI) Index(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	page, _ := strconv.Atoi(r.Form.Get(GetParamPage))
	perPage, _ := strconv.Atoi(r.Form.Get(GetParamPerPage))

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

	b, _ := json.Marshal(registry)
	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(b)

	return
}

// Create is a handler for creating a new DataSource
func (regAPI *RegistryAPI) Create(w http.ResponseWriter, r *http.Request) {

	//	contentType := strings.Split(r.Header.Get("Content-Type"), ";")[0]
	//	if contentType != "application/json" {
	//		common.ErrorResponse(http.StatusUnsupportedMediaType, "Unsupported content type: "+contentType, w)
	//		return
	//	}

	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	// Validate the unmarshalled DataSource
	err = validateDataSource(&ds, CREATE)
	if err != nil {
		common.ErrorResponse(http.StatusConflict, err.Error(), w)
		return
	}

	err = regAPI.storage.add(&ds)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error storing the datasource: "+err.Error(), w)
		return
	}

	w.Header().Set("Location", ds.URL)
	w.WriteHeader(http.StatusCreated)
	return
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	ds, err := regAPI.storage.get(id)
	if err == ErrorNotFound {
		common.ErrorResponse(http.StatusNotFound, "Error: "+err.Error(), w)
		return
	} else if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error requesting registry: "+err.Error(), w)
		return
	}

	b, _ := json.Marshal(ds)

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(b)

	return
}

// Update is a handler for updating the given DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Update(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	// Validate the unmarshalled DataSource
	err = validateDataSource(&ds, UPDATE)
	if err != nil {
		common.ErrorResponse(http.StatusConflict, err.Error(), w)
		return
	}

	err = regAPI.storage.update(id, &ds)
	if err != nil {
		common.ErrorResponse(httpNotFound, err.Error(), w)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

// Delete is a handler for deleting the given DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Delete(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	err := regAPI.storage.delete(id)
	if err != nil {
		common.ErrorResponse(httpNotFound, err.Error(), w)
		return
	}

	return
}

// Filter is a handler for registry filtering API
// Expected parameters: path, type, op, value
func (regAPI *RegistryAPI) Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	ftype := params["type"]
	fop := params["op"]
	fvalue := params["value"]

	r.ParseForm()
	page, _ := strconv.Atoi(r.Form.Get(GetParamPage))
	perPage, _ := strconv.Atoi(r.Form.Get(GetParamPerPage))
	page, perPage = common.ValidatePagingParams(page, perPage, MaxPerPage)

	var body []byte
	switch ftype {
	case FTypeOne:
		datasource, err := regAPI.storage.pathFilterOne(fpath, fop, fvalue)
		if err != nil {
			common.ErrorResponse(http.StatusBadRequest, "Error processing the request: "+err.Error(), w)
			return
		}

		if datasource.ID != "" {
			body, _ = json.Marshal(datasource)
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
			body, _ = json.Marshal(registry)
		} else {
			common.ErrorResponse(http.StatusNotFound, "No matched entries found.", w)
			return
		}
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.Write(body)
}

///////////////////////////////////////////////////////////////////////////////////

////
const (
	CREATE uint8 = iota
	UPDATE
)

// Validate the DataSource for:
// 	Create:
//	- Not provided: id, url, data
//	- Provided: resource, type, format
//	- Valid: type, retention.policy, retention.duration, aggregates
//	Update:
//	- Not provided: id, url, data, resource, type
//	- Provided: format
//	- Valid: retention.policy, retention.duration, aggregates
///////////////////////////////////////////////////////////////////////////////////////
// TODO refactor the validations based on attributes rather than the type of validation
func validateDataSource(ds *DataSource, context uint8) error {
	var _errors []string

	//// System generated (Read-only) ////////////////////////////////////////////
	var readOnlyKeys []string
	if ds.ID != "" {
		readOnlyKeys = append(readOnlyKeys, "id")
	}
	if ds.URL != "" {
		readOnlyKeys = append(readOnlyKeys, "url")
	}
	if ds.Data != "" {
		readOnlyKeys = append(readOnlyKeys, "data")
	}

	///// Fixed (Read-only once created) /////////////////////////////////////////
	if context == UPDATE {
		if ds.Resource != "" {
			readOnlyKeys = append(readOnlyKeys, "resource")
		}
		if ds.Type != "" {
			readOnlyKeys = append(readOnlyKeys, "type")
		}
	}

	if len(readOnlyKeys) > 0 {
		_errors = append(_errors, "Ambitious assignment to read-only key(s): "+strings.Join(readOnlyKeys, ", "))
	}

	///// Mandatory ///////////////////////////////////////////////////////////////
	var mandatoryKeys []string
	if context == CREATE {
		if ds.Resource == "" {
			mandatoryKeys = append(mandatoryKeys, "resource")
		}
		if ds.Type == "" {
			mandatoryKeys = append(mandatoryKeys, "type")
		}
	}
	if ds.Format == "" {
		mandatoryKeys = append(mandatoryKeys, "format")
	}

	if len(mandatoryKeys) > 0 {
		_errors = append(_errors, "Missing mandatory value(s) of: "+strings.Join(mandatoryKeys, ", "))
	}

	//// Invalid //////////////////////////////////////////////////////////////////
	var invalidKeys []string
	if ds.Resource != "" {
		_, err := url.Parse(ds.Resource)
		if err != nil {
			invalidKeys = append(invalidKeys, "resource")
		}
	}
	if ds.Retention.Policy != "" {
		if !validateRetention(ds.Retention.Policy) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("retention.policy<[0-9]*(%s)>", strings.Join(common.RetentionPeriods(), "|")))
		}
	}
	if ds.Retention.Duration != "" {
		if !validateRetention(ds.Retention.Duration) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("retention.duration<[0-9]*(%s)>", strings.Join(common.RetentionPeriods(), "|")))
		}
	}
	if ds.Type != "" {
		if !stringInSlice(ds.Type, common.SupportedTypes()) {
			invalidKeys = append(invalidKeys, fmt.Sprintf("type<%s>", strings.Join(common.SupportedTypes(), ",")))
		}
	}
	// Todo: Validate ds.Aggregation
	// common.SupportedAggregates()
	// only if format=float

	if len(invalidKeys) > 0 {
		_errors = append(_errors, "Invalid value(s) for: "+strings.Join(invalidKeys, ", "))
	}

	///// return if any errors ////////////////////////////////////////////////////
	if len(_errors) > 0 {
		return errors.New(strings.Join(_errors, ". "))
	}

	return nil
}

func validateRetention(retention string) bool {
	// Create regexp: h|d|w|m
	retPeriods := strings.Join(common.RetentionPeriods(), "|")
	// Create regexp: ^[0-9]*(h|d|w|m)$
	re := regexp.MustCompile("^[0-9]*(" + retPeriods + ")$")

	return re.MatchString(retention)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
