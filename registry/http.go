package registry

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

// Registry api
type RegistryAPI struct {
	storage Storage
	ntChan  chan<- common.Notification // write-only channel
}

func NewRegistryAPI(storage Storage, ntChan chan<- common.Notification) *RegistryAPI {
	return &RegistryAPI{
		storage,
		ntChan,
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

	addedDS, err := regAPI.storage.add(ds)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error storing the datasource: "+err.Error(), w)
		return
	}

	// Send a create notification
	regAPI.sendNotification(&addedDS, common.CREATE)

	//b, _ := json.Marshal(addedDS)
	w.Header().Set("Location", addedDS.URL)
	//w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusCreated)
	//w.Write(b)

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

	oldDS, err := regAPI.storage.get(id)
	if err != nil {
		common.ErrorResponse(httpNotFound, err.Error(), w)
		return
	}
	updatedDS, err := regAPI.storage.update(id, ds)

	// Compare DataAPI-related changes
	if oldDS.Retention != updatedDS.Retention { // anything else?
		// Send an update notification
		regAPI.sendNotification(&updatedDS, common.UPDATE_DATA)
	}
	// Compare AggrAPI-related changes
	//	if(oldDS.Aggregation != updatedDS.Aggregation){
	//		// Send an update notification
	//		regAPI.sendNotification(&updatedDS, common.UPDATE_AGGR)
	//	}

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

	// Send a delete notification
	regAPI.sendNotification(&DataSource{ID: id}, common.DELETE)

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

// Sends a Notification{} to channel
func (regAPI *RegistryAPI) sendNotification(ds *DataSource, _type common.NotificationTYPE) {
	regAPI.ntChan <- common.Notification{DS: *ds, TYPE: _type}
}