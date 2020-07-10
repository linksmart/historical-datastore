// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	controller *Controller
}

// NewAPI returns the configured Data API
func NewAPI(registry registry.Storage, storage Storage, autoRegistration bool) *API {
	return &API{NewController(registry, storage, autoRegistration)}
}

// QueryPage is a handler for querying data
// Expected parameters: id(s), optional: pagination, query string
func (api *API) Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	timeStart := time.Now()
	params := mux.Vars(r)
	var recordSet RecordSet

	// Parse id(s) and get sources from registry
	ids := strings.Split(params["id"], common.IDSeparator)

	// Parse query
	q, err := ParseQueryParameters(r.Form)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing query parameters:" + err.Error()}, w)
		return
	}

	data, total, err := api.controller.QueryPage(q, ids)
	if err != nil {
		common.HttpErrorResponse(err, w)
		return
	}

	curLink := common.DataAPILoc + "/" + GetUrlFromQuery(q, ids...)

	nextLink := ""

	responseLength := len(data)

	//If the response is already less than the number of elements supposed to be in a page,
	//then it already means that we are in last page
	if responseLength >= q.PerPage {
		nextQuery := q
		nextQuery.Page = q.Page + 1
		nextLink = common.DataAPILoc + "/" + GetUrlFromQuery(nextQuery, ids...)
	}

	recordSet = RecordSet{
		SelfLink: curLink,
		TimeTook: time.Since(timeStart).Seconds(),
		Data:     data,
		NextLink: nextLink,
		Count:    total,
	}

	csvStr, errMarshal := json.Marshal(recordSet)
	if errMarshal != nil {
		common.HttpErrorResponse(&common.InternalError{S: "Error marshalling recordset: " + errMarshal.Error()}, w)
		return
	}

	w.Header().Add("Content-Type", common.DefaultMIMEType)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(csvStr))
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
func (api *API) Submit(w http.ResponseWriter, r *http.Request) {
	// Read body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	// Parse payload
	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		contentType, _, err = mime.ParseMediaType(contentType)
		if err != nil {
			common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing Content-Type header: " + err.Error()}, w)
			return
		}
	}
	decoder, err := getDecoderForContentType(contentType)
	if err != nil {
		common.HttpErrorResponse(&common.UnsupportedMediaTypeError{S: "Error parsing Content-Type:" + err.Error()}, w)
		return
	}

	senmlPack, err := decoder(body)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing message body: " + err.Error()}, w)
		return
	}

	params := mux.Vars(r)
	// Parse id(s) and get streams from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	submitErr := api.controller.submit(senmlPack, ids)
	if submitErr != nil {
		common.HttpErrorResponse(submitErr, w)
	} else {
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
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	// Parse payload
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing Content-Type header: "}, w)
		return
	}

	if contentType == "" {
		common.HttpErrorResponse(&common.BadRequestError{S: "Missing Content-Type"}, w)
		return
	}

	decoder, err := getDecoderForContentType(contentType)
	if err != nil {
		common.HttpErrorResponse(&common.UnsupportedMediaTypeError{S: "Error parsing Content-Type:" + err.Error()}, w)
		return
	}

	senmlPack, err := decoder(body)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing message body: " + err.Error()}, w)
		return
	}

	submitErr := api.controller.submit(senmlPack, nil)
	if submitErr != nil {
		common.HttpErrorResponse(submitErr, w)
	} else {
		w.Header().Set("Content-Type", common.DefaultMIMEType)
		w.WriteHeader(http.StatusAccepted)
	}
	return
}

func (api *API) Delete(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	params := mux.Vars(r)

	// Parse id(s) and get sources from registry
	streams := strings.Split(params["id"], common.IDSeparator)

	from, err := parseFromValue(r.Form.Get(common.ParamFrom))
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing From value:" + err.Error()}, w)
		return
	}

	// end time
	to, err := parseToValue(r.Form.Get(common.ParamTo))
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: "Error parsing to value:" + err.Error()}, w)
		return
	}
	commonErr := api.controller.Delete(streams, from, to)
	if commonErr != nil {
		common.HttpErrorResponse(commonErr, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
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
		return nil, fmt.Errorf("unsupported Content-Type:%s", contentType)
	}
	return decoder, nil
}

func GetUrlFromQuery(q Query, id ...string) (url string) {
	var sort, limit, start, end, perPage, offset, denorm string
	if q.SortAsc {
		sort = fmt.Sprintf("&%v=%v", common.ParamSort, common.Asc)
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
		if q.Denormalize&DenormMaskTime != 0 {
			denorm += TimeFieldShort + ","
		}
		if q.Denormalize&DenormMaskName != 0 {
			denorm += NameFieldShort + ","
		}
		if q.Denormalize&DenormMaskUnit != 0 {
			denorm += UnitFieldShort + ","
		}
		if q.Denormalize&DenormMaskSum != 0 {
			denorm += SumFieldShort + ","
		}
		if q.Denormalize&DenormMaskValue != 0 {
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

// Utility functions

func ParseQueryParameters(form url.Values) (Query, common.Error) {
	q := Query{}
	var err error

	// start time
	q.To, err = parseFromValue(form.Get(common.ParamFrom))
	if err != nil {
		return Query{}, &common.BadRequestError{S: "Error parsing From value:" + err.Error()}
	}

	// end time

	q.To, err = parseToValue(form.Get(common.ParamTo))
	if err != nil {
		return Query{}, &common.BadRequestError{S: "Error parsing to value:" + err.Error()}
	}

	q.Page, q.PerPage, err = common.ParsePagingParams(form.Get(common.ParamPage), form.Get(common.ParamPerPage), MaxPerPage)

	if err != nil {
		return Query{}, &common.BadRequestError{S: "Error parsing limit argument:" + err.Error()}
	}

	// sort
	sort := form.Get(common.ParamSort)
	if sort == common.Asc {
		// default sorting order
		q.SortAsc = true
	} else if sort != "" && sort != common.Asc && sort != common.Desc {
		return Query{}, &common.BadRequestError{S: "Invalid sort argument:" + sort}
	} //else sortAsc is false

	//denormalization fields
	denormStr := form.Get(common.ParamDenormalize)
	q.Denormalize, err = parseDenormParams(denormStr)
	if err != nil {
		return Query{}, &common.BadRequestError{S: fmt.Sprintf("error in param %s=%s:%v", common.ParamDenormalize, denormStr, err)}
	}

	//get Count
	if strings.EqualFold(form.Get(common.ParamCount), "true") {
		q.Count = true
	}

	return q, nil
}
