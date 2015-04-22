package data

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	senml "github.com/krylovsk/gosenml"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

// DataAPI describes the HTTP data API
type DataAPI struct {
	registryClient registry.Client
	storage        Storage
}

// NewDataAPI returns the configured Data API
func NewDataAPI(registryClient registry.Client, storage Storage) *DataAPI {
	return &DataAPI{
		registryClient,
		storage,
	}
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
func (d *DataAPI) Submit(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	data := make(map[string][]DataPoint)
	sources := make(map[string]registry.DataSource)
	var senmlMessage senml.Message
	contentType := strings.Split(r.Header.Get("Content-Type"), ";")[0]

	// Only SenML is supported for now
	if contentType != "application/senml+json" {
		common.ErrorResponse(http.StatusUnsupportedMediaType, "Unsupported content type", w)
		return
	}

	// Parse id(s)
	ids := strings.Split(params["id"], common.IDSeparator)

	// Parse payload
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	err := decoder.Decode(&senmlMessage)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	// Check if DataSources are registered in the Registry
	dsResources := make(map[string]registry.DataSource)
	for _, id := range ids {
		ds, err := d.registryClient.Get(id)
		if err != nil {
			log.Printf("Error retrieving data source %v from the registry: %v **data will be discarded**", id, err.Error())
			continue
		}
		dsResources[ds.Resource.String()] = *ds
	}

	// Fill the data map with provided data points
	entries := senmlMessage.Expand().Entries
	for _, e := range entries {
		// Check if there is a data source for this entry
		ds, ok := dsResources[e.Name]
		if !ok {
			log.Printf("Data point for unknown data source %v **data will be discarded**", e.Name)
			// continue
		}
		_, ok = data[ds.ID]
		if !ok {
			data[ds.ID] = []DataPoint{}
			sources[ds.ID] = ds
		}
		data[ds.ID] = append(data[ds.ID], NewDataPoint(e))
	}

	// Add data to the storage
	err = d.storage.submit(data, sources)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error writing data to the database: "+err.Error(), w)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	return
}

// Query is a handler for querying data
// Expected parameters: id(s), optional: pagination, query string
func (d *DataAPI) Query(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	query := params["query"]
	// page := params["page"]
	// perPage := params["per_page"]

	// TODO
	fmt.Fprintf(w, "TODO data query id: %v query: %v", id, query)
}
