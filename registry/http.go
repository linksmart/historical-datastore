package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

// Registry api
type RegistryAPI struct {
	storage *MemoryStorage
}

func NewRegistryAPI() *RegistryAPI {

	return &RegistryAPI{
		storage: NewMemoryStorage(),
	}
}

const (
	GetParamPage    = "page"
	GetParamPerPage = "per_page"
	// Max DataSources displayed in each page of registry
	MaxPerPage = 100
	//DefaultMIMEType = "application/vnd.eu.linksmart.hds+json;version=" + common.APIVersion
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
	err = unmarshalDataSource(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	// Validate the unmarshalled DataSource
	err = validateWritableDataSource(&ds)
	if err != nil {
		common.ErrorResponse(http.StatusConflict, "Invalid input: "+err.Error(), w)
		return
	}

	err = regAPI.storage.add(&ds)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error storing the datasource: "+err.Error(), w)
		return
	}

	//fmt.Printf("%+v\n", ds)

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
	err = unmarshalDataSource(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error processing input: "+err.Error(), w)
		return
	}

	// Validate the unmarshalled DataSource
	err = validateWritableDataSource(&ds)
	if err != nil {
		common.ErrorResponse(http.StatusConflict, "Invalid input: "+err.Error(), w)
		return
	}

	err = regAPI.storage.update(id, &ds)
	if err != nil {
		common.ErrorResponse(404 /* NotFound */, err.Error(), w)
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
		common.ErrorResponse(404 /* NotFound */, err.Error(), w)
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
	// TODO
	fmt.Fprintf(w, "TODO registry filter %v/%v/%v/%v", fpath, ftype, fop, fvalue)
}

///////////////////////////////////////////////////////////////////////////////////

// Unmarshalls json and parses the string of resource url
func unmarshalDataSource(body []byte, ds *DataSource) error {
	// Unmarshal body
	err := json.Unmarshal(body, ds)
	if err != nil {
		return err
	}

	// Unmarshal the resource string seperately
	type RawDataSource struct {
		Resource string `json:"resource"`
	}
	var rds RawDataSource
	err = json.Unmarshal(body, &rds)
	if err != nil {
		return err
	}

	// Parse it into URL
	resourceURL, err := url.Parse(rds.Resource)
	if err != nil {
		return err
	}

	// Add it to DataSource
	ds.Resource = *resourceURL

	return nil
}

// Validate that only writable DataSource elements are provided and that are valid
func validateWritableDataSource(ds *DataSource) error {

	// Make sure no read-only data is being added/modified
	var illegal_entities []string
	if ds.ID != "" {
		illegal_entities = append(illegal_entities, "id")
	}
	if ds.URL != "" {
		illegal_entities = append(illegal_entities, "url")
	}
	if ds.Data != "" {
		illegal_entities = append(illegal_entities, "data")
	}
	if len(illegal_entities) > 0 {
		return errors.New("Conflicting read-only entities: " + strings.Join(illegal_entities, ", "))
	}

	// todo: validate other entities

	return nil
}
