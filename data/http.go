// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/gorilla/mux"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	errors2 "github.com/syndtr/goleveldb/leveldb/errors"
)

const (
	MaxPerPage = 1000

	//value for ParamDenormalize
	TimeField       = "time"
	TimeFieldShort  = "t"
	NameField       = "name"
	NameFieldShort  = "n"
	UnitField       = "unit"
	UnitFieldShort  = "u"
	ValueField      = "value"
	ValueFieldShort = "v"
	SumField        = "sum"
	SumFieldShort   = "s"
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

func getDecoderForContentType(contentType string) (decoder codec.Decoder, err error) {
	decoderMap := map[string]codec.Decoder{
		"":                            codec.DecodeJSON,
		"application/senml+json":      codec.DecodeJSON,
		"application/json":            codec.DecodeJSON,
		"application/senml+cbor":      codec.DecodeCBOR,
		"application/cbor":            codec.DecodeCBOR,
		"application/senml+xml":       codec.DecodeXML,
		"application/xml":             codec.DecodeXML,
		"text/csv":                    codec.DecodeCSV,
		senml.MediaTypeCustomSenmlCSV: codec.DecodeCSV,
	}

	decoder, ok := decoderMap[contentType]
	if !ok {
		return nil, fmt.Errorf("Unsupported Content-Type:%s", contentType)
	}
	return decoder, nil
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
func (api *API) Submit(w http.ResponseWriter, r *http.Request) {
	// Read body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	// Parse payload
	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		contentType, _, err = mime.ParseMediaType(contentType)
		if err != nil {
			common.ErrorResponse(http.StatusBadRequest, "Error parsing Content-Type header: "+err.Error(), w)
			return
		}
	}
	decoder, err := getDecoderForContentType(contentType)
	if err != nil {
		common.ErrorResponse(http.StatusUnsupportedMediaType, "Error parsing Content-Type:"+err.Error(), w)
		return
	}

	senmlPack, err := decoder(body)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	params := mux.Vars(r)
	// Parse id(s) and get streams from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	streams := make(map[string]*registry.DataStream)
	for _, id := range ids {
		ds, err := api.registry.Get(id)
		if err != nil {
			common.ErrorResponse(http.StatusNotFound,
				fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		streams[ds.Name] = ds
	}

	code, err := AddToStorage(senmlPack, api.storage, api.registry, streams, false)

	if err != nil {
		common.ErrorResponse(code, err.Error(), w)
	} else {
		w.Header().Set("Content-Type", common.DefaultMIMEType)
		w.WriteHeader(http.StatusAccepted)
	}
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
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing Content-Type header: "+err.Error(), w)
		return
	}

	if contentType == "" {
		common.ErrorResponse(http.StatusBadRequest, "Missing Content-Type", w)
		return
	}

	decoder, err := getDecoderForContentType(contentType)
	if err != nil {
		common.ErrorResponse(http.StatusUnsupportedMediaType, "Error parsing Content-Type:"+err.Error(), w)
		return
	}

	senmlPack, err := decoder(body)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, "Error parsing message body: "+err.Error(), w)
		return
	}

	code, err := AddToStorage(senmlPack, api.storage, api.registry, nil, api.autoRegistration)
	if err != nil {
		common.ErrorResponse(code, err.Error(), w)
	} else {
		w.Header().Set("Content-Type", common.DefaultMIMEType)
		w.WriteHeader(http.StatusAccepted)
	}
	return
}

func AddToStorage(senmlPack senml.Pack, storage Storage, regStorage registry.Storage, streams map[string]*registry.DataStream, autoRegistration bool) (code int, err error) {
	// map of resource name -> Data stream
	nameDSs := make(map[string]*registry.DataStream)
	fromStreamList := false
	if streams != nil {
		nameDSs = streams
		fromStreamList = true
	}
	// Fill the data map with provided data points
	data := make(map[string]senml.Pack)
	senmlPack.Normalize()
	for _, r := range senmlPack {
		ds, found := nameDSs[r.Name]
		if !found && fromStreamList {
			return http.StatusBadRequest, fmt.Errorf("senml entry %s does not match the provided datastream", r.Name)
		}
		if !found {
			ds, err := regStorage.Get(r.Name)
			if err != nil {
				if errors.Is(err, errors2.ErrNotFound) {
					if !autoRegistration {
						return http.StatusNotFound, fmt.Errorf("Data stream with name %v is not registered.", r.Name)

					}

					// Register a Data stream with this name
					log.Printf("Registering Data stream for %s", r.Name)
					newDS := registry.DataStream{
						Name: r.Name,
						Unit: r.Unit,
					}
					if r.Value != nil || r.Sum != nil {
						newDS.Type = registry.Float
					} else if r.StringValue != "" {
						newDS.Type = registry.String
					} else if r.BoolValue != nil {
						newDS.Type = registry.Bool
					} else if r.DataValue != "" {
						newDS.Type = registry.Data
					}
					addedDS, err := regStorage.Add(newDS)
					if err != nil {
						return http.StatusBadRequest, fmt.Errorf("Error registering %v in the registry: %v", r.Name, err)

					}
					ds = addedDS
				}
				nameDSs[r.Name] = ds
			} else {
				return http.StatusInternalServerError, err
			}
		}

		err := validateRecordAgainstRegistry(r, ds)

		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("Error validating the record:%v", err)
		}

		// Prepare for storage
		_, found = data[ds.Name]
		if !found {
			data[ds.Name] = senml.Pack{}
		}
		data[ds.Name] = append(data[ds.Name], r)
	}

	// Add data to the storage
	err = storage.Submit(data, nameDSs)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("error writing data to the database: " + err.Error())
	}
	return http.StatusAccepted, nil
}

func GetUrlFromQuery(q Query, id ...string) (url string) {
	var sort, limit, start, end, perPage, offset, denorm string
	if q.Sort != "" {
		sort = fmt.Sprintf("&%v=%v", common.ParamSort, q.Sort)
	}
	if !q.From.IsZero() {
		start = fmt.Sprintf("&%v=%v", common.ParamFrom, q.From.UTC().Format(time.RFC3339))
	}
	if !q.To.IsZero() {
		end = fmt.Sprintf("&%v=%v", common.ParamTo, q.To.UTC().Format(time.RFC3339))
	}
	if q.Page > 0 {
		offset = fmt.Sprintf("&%v=%v", common.ParamPage, q.Page)
	}
	if q.PerPage > 0 {
		perPage = fmt.Sprintf("&%v=%v", common.ParamPerPage, q.PerPage)
	}

	if q.Denormalize != 0 {
		denorm = fmt.Sprintf("&%v=", common.ParamDenormalize)
		if q.Denormalize&FTime != 0 {
			denorm += TimeFieldShort + ","
		}
		if q.Denormalize&FName != 0 {
			denorm += NameFieldShort + ","
		}
		if q.Denormalize&FUnit != 0 {
			denorm += UnitFieldShort + ","
		}
		if q.Denormalize&FSum != 0 {
			denorm += SumFieldShort + ","
		}
		if q.Denormalize&FValue != 0 {
			denorm += ValueFieldShort + ","
		}
		denorm = strings.TrimSuffix(denorm, ",")
	}
	return fmt.Sprintf("%v?%s%s%s%s%s%s%s",
		strings.Join(id, common.IDSeparator),
		perPage,
		sort, limit, start, end, offset, denorm,
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
				fmt.Sprintf("Error retrieving Data stream %v from the registry: %v", id, err.Error()),
				w)
			return
		}
		sources = append(sources, ds)
	}
	if len(sources) == 0 {
		common.ErrorResponse(http.StatusNotFound,
			"None of the specified Data streams could be retrieved from the registry.", w)
		return
	}

	// Parse query
	q, err := ParseQueryParameters(r.Form)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, err.Error(), w)
		return
	}

	data, total, err := api.storage.Query(q, sources...)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, "Error retrieving data from the database: "+err.Error(), w)
		return
	}

	curlink := common.DataAPILoc + "/" + GetUrlFromQuery(q, ids...)

	nextlink := ""

	responseLength := len(data)

	//If the response is already less than the number of elements supposed to be in a page,
	//then it already means that we are in last page
	if responseLength >= q.PerPage {
		nextQuery := q
		nextQuery.Page = q.Page + 1
		nextlink = common.DataAPILoc + "/" + GetUrlFromQuery(nextQuery, ids...)
	}

	recordSet = RecordSet{
		SelfLink: curlink,
		TimeTook: time.Since(timeStart).Seconds(),
		Data:     data,
		NextLink: nextlink,
		Count:    total,
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

	q.Page, q.PerPage, err = common.ParsePagingParams(form.Get(common.ParamPage), form.Get(common.ParamPerPage), MaxPerPage)

	if err != nil {
		return Query{}, fmt.Errorf("Error parsing limit argument: %s", err)
	}

	// sort
	q.Sort = form.Get(common.ParamSort)
	if q.Sort == "" {
		// default sorting order
		q.Sort = common.Desc
	} else if q.Sort != common.Asc && q.Sort != common.Desc {
		return Query{}, fmt.Errorf("Invalid sort argument: %v", q.Sort)
	}

	q.Page, q.PerPage, err = common.ParsePagingParams(form.Get(common.ParamPage), form.Get(common.ParamPerPage), MaxPerPage)

	if err != nil {
		return Query{}, fmt.Errorf("Error parsing paging parameters: %s", err)
	}

	//denormalization fields
	denormStr := form.Get(common.ParamDenormalize)
	q.Denormalize, err = parseDenormParams(denormStr)
	if err != nil {
		return Query{}, fmt.Errorf("error in param %s=%s:%v", common.ParamDenormalize, denormStr, err)
	}

	//get count
	if strings.EqualFold(form.Get(common.ParamCount), "true") {
		q.count = true
	}
	return q, nil
}

func parseDenormParams(denormString string) (denormMask DenormMask, err error) {

	if denormString != "" {
		denormStrings := strings.Split(denormString, ",")
		for _, field := range denormStrings {
			switch strings.ToLower(strings.TrimSpace(field)) {
			case TimeField, TimeFieldShort:
				denormMask = denormMask | FTime
			case NameField, NameFieldShort:
				denormMask = denormMask | FName
			case UnitField, UnitFieldShort:
				denormMask = denormMask | FUnit
			case ValueField, ValueFieldShort:
				denormMask = denormMask | FValue
			case SumField, SumFieldShort:
				denormMask = denormMask | FSum
			default:
				return 0, fmt.Errorf("unexpected senml field: %s", field)

			}
		}
	}
	return denormMask, nil
}
