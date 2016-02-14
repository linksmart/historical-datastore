package aggregation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"linksmart.eu/lc/core/catalog"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)

const (
	// MaxPerPage defines the maximum number of results returned per page
	MaxPerPage = 100
)

type API struct {
	registryClient registry.Client
	storage        Aggr
}

func NewAPI(registryClient registry.Client, storage Aggr) *API {
	return &API{registryClient, storage}
}

// Retrieve aggregations from registry api
func (api *API) Aggregations() (map[string]Aggregation, error) {
	aggrs := make(map[string]Aggregation)
	perPage := 100
	for page := 1; ; page++ {
		datasources, total, err := api.registryClient.GetDataSources(page, perPage)
		if err != nil {
			return aggrs, err
		}

		for _, ds := range datasources {
			for _, dsa := range ds.Aggregation {
				var aggr Aggregation
				aggr.ID = dsa.ID
				aggr.Interval = dsa.Interval
				aggr.Aggregates = dsa.Aggregates
				aggr.Retention = dsa.Retention
				var sources []string
				a, found := aggrs[dsa.ID]
				if found {
					sources = a.Sources
				}
				aggr.Sources = append(sources, ds.ID)
				aggrs[dsa.ID] = aggr
			}
		}

		if page*perPage >= total {
			break
		}
	}

	return aggrs, nil
}

func (api *API) Index(w http.ResponseWriter, r *http.Request) {

	aggrs, err := api.Aggregations()
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error reading registry: "+err.Error(), w)
		return
	}

	var index Index
	index.Aggrs = make([]Aggregation, 0, len(aggrs))
	for _, v := range aggrs {
		index.Aggrs = append(index.Aggrs, v)
	}

	b, err := json.Marshal(&index)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error marshalling: "+err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func (api *API) Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	fop := params["op"]
	fvalue := params["value"]
	pathTknz := strings.Split(fpath, ".")

	aggrs, err := api.Aggregations()
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error reading registry: "+err.Error(), w)
		return
	}

	var index Index
	index.Aggrs = make([]Aggregation, 0, len(aggrs))
	for _, aggr := range aggrs {
		matched, err := catalog.MatchObject(aggr, pathTknz, fop, fvalue)
		if err != nil {
			common.ErrorResponse(http.StatusInternalServerError, "Error matching aggregation: "+err.Error(), w)
			return
		}
		if matched {
			index.Aggrs = append(index.Aggrs, aggr)
		}
	}

	b, err := json.Marshal(&index)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error marshalling: "+err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

func (api *API) Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	timeStart := time.Now()
	params := mux.Vars(r)

	aggrID := params["aggrid"]

	var (
		page, perPage int
		//recordSet     RecordSet
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
	ids := strings.Split(params["uuid"], common.IDSeparator)
	if len(ids) == 0 {
		common.ErrorResponse(http.StatusBadRequest,
			"Source IDs not specified.", w)
		return
	}

	var aggr registry.AggregatedDataSource
	var sources []registry.DataSource
OUTERLOOP:
	for _, id := range ids {
		ds, err := api.registryClient.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving data source %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		for _, dsa := range ds.Aggregation {
			if dsa.ID == aggrID {
				sources = append(sources, ds)
				aggr = dsa
				continue OUTERLOOP
			}
		}

		sources = append(sources, ds)
	}

	// Parse query
	q := data.ParseQueryParameters(r.Form)

	// perPage should be at least len(sources), i.e., one point per resource
	if perPage < len(sources) {
		common.ErrorResponse(http.StatusBadRequest,
			"per_page must be greater than the number of queried sources.", w)
		return
	}

	dataset, total, err := api.storage.Query(aggr, q, page, perPage, sources...)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
		return
	}

	v := url.Values{}
	v.Add("start", q.Start.Format(time.RFC3339))
	// Omit end in open-ended queries
	if q.End.After(q.Start) {
		v.Add("end", q.End.Format(time.RFC3339))
	}
	v.Add("sort", q.Sort)
	if q.Limit > 0 { // non-positive limit is ignored
		v.Add("limit", fmt.Sprintf("%d", q.Limit))
	}
	v.Add("page", fmt.Sprintf("%d", page))
	v.Add("per_page", fmt.Sprintf("%d", perPage))
	recordSet := RecordSet{
		URL:     fmt.Sprintf("%s?%s", r.URL.Path, v.Encode()),
		Data:    dataset,
		Time:    time.Since(timeStart).Seconds() * 1000,
		Page:    page,
		PerPage: perPage,
		Total:   total,
	}

	b, err := json.Marshal(recordSet)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error marshalling recordset: "+err.Error(), w)
		return
	}

	w.Header().Set("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}
