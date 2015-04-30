package data

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	senml "linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/krylovsk/gosenml"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/registry"
)

const (
	// MaxPerPage defines the maximum number of results returned per page
	MaxPerPage = 100
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

func parseQueryParameters(form url.Values) query {
	q := query{}
	var err error

	// if erroneous time specified for start use 'zero time'
	q.start, err = time.Parse(time.RFC3339, form.Get("start"))
	if err != nil {
		q.start = time.Time{}
	}

	// if erroneous time specified for end use 'now'
	q.end, err = time.Parse(time.RFC3339, form.Get("end"))
	if err != nil {
		q.end = time.Time{}
	}

	// limit shall be int
	q.limit, err = strconv.Atoi(form.Get("limit"))
	if err != nil {
		q.limit = -1
	}

	// sort shall be asc or desc
	q.sort = form.Get("sort")
	if q.sort == "" || q.sort != ASC {
		q.sort = DESC
	}
	return q
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
		dsResources[ds.Resource] = *ds
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
		p := NewDataPoint()
		data[ds.ID] = append(data[ds.ID], p.FromEntry(e))
	}

	// Add data to the storage
	err = d.storage.submit(data, sources)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error writing data to the database: "+err.Error(), w)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.eu.linksmart.hds+json;version="+common.APIVersion)
	w.WriteHeader(http.StatusAccepted)
	return
}

// Query is a handler for querying data
// Expected parameters: id(s), optional: pagination, query string
func (d *DataAPI) Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	timeStart := time.Now()
	params := mux.Vars(r)
	var (
		page, perPage int
		recordSet     RecordSet
	)

	page, err := strconv.Atoi(r.Form.Get("page"))
	if err != nil {
		page = 1
	}
	perPage, err = strconv.Atoi(r.Form.Get("per_page"))
	if err != nil {
		perPage = MaxPerPage
	}
	page, perPage = common.ValidatePagingParams(page, perPage, MaxPerPage)

	// Parse id(s) and get sources from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	sources := []registry.DataSource{}
	for _, id := range ids {
		ds, err := d.registryClient.Get(id)
		if err != nil {
			log.Printf("Error retrieving data source %v from the registry: %v **data will be discarded**", id, err.Error())
			continue
		}
		sources = append(sources, *ds)
	}

	// no parameters - return last values
	if len(r.Form) == 0 {
		data, err := d.storage.getLast(sources...)
		if err != nil {
			common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
			return
		}

		recordSet = RecordSet{
			URL:     fmt.Sprintf("%s", r.URL.Path),
			Data:    data,
			Time:    time.Since(timeStart).Seconds() * 1000,
			Error:   "",
			Page:    page,
			PerPage: perPage,
			Total:   len(data.Entries),
		}

	} else {
		// Parse query
		q := parseQueryParameters(r.Form)

		// perPage should be at least len(sources), i.e., one point per resource
		if perPage < len(sources) {
			perPage = len(sources)
		}

		data, total, err := d.storage.query(q, page, perPage, sources...)
		if err != nil {
			common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
			return
		}

		v := url.Values{}
		v.Add("start", q.start.Format(time.RFC3339))
		v.Add("end", q.end.Format(time.RFC3339))
		v.Add("sort", q.sort)
		v.Add("limit", fmt.Sprintf("%d", q.limit))
		v.Add("page", fmt.Sprintf("%d", page))
		v.Add("per_page", fmt.Sprintf("%d", perPage))
		recordSet = RecordSet{
			URL:     fmt.Sprintf("%s?%s", r.URL.Path, v.Encode()),
			Data:    data,
			Time:    time.Since(timeStart).Seconds() * 1000,
			Error:   "",
			Page:    page,
			PerPage: perPage,
			Total:   total,
		}
	}
	b, _ := json.Marshal(recordSet)

	w.Header().Set("Content-Type", "application/vnd.eu.linksmart.hds+json;version="+common.APIVersion)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
