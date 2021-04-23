// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/gorilla/mux"
	"github.com/linksmart/historical-datastore/common"
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
	c Controller
}

// NewAPI returns the configured Data API
func NewAPI(c Controller) *API {
	return &API{c: c}
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

	data, total, err := api.c.QueryPage(r.Context(), q, ids)
	if err != nil {
		common.HttpErrorResponse(err, w)
		return
	}

	baseLink := fmt.Sprintf("%s/%s?", common.DataAPILoc, params["id"])
	form := getFormFromQuery(q)
	curLink := baseLink + form.Encode()

	nextLink := ""

	responseLength := len(data)

	//If the response is already less than the number of elements supposed to be in a page,
	//then it already means that we are in last page
	if responseLength >= q.PerPage {
		form.Set(common.ParamPage, strconv.Itoa(q.Page+1))
		form.Del(common.ParamCount)
		nextLink = baseLink + form.Encode()
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
	w.Write(csvStr)
}

func getRequestBodyReader(r *http.Request) (io.Reader, common.Error) {
	var reader io.ReadCloser
	contentEncoding := r.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		var err error
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			return nil, &common.BadRequestError{S: fmt.Sprintf("error parsing the http body: %s", err)}
		}
	case "":
		reader = r.Body
	default:
		return nil, &common.BadRequestError{S: fmt.Sprintf("unsupported Content-Encoding %s", contentEncoding)}
	}
	return reader, nil
}

// Submit is a handler for submitting a new data point
// Expected parameters: id(s)
func (api *API) Submit(w http.ResponseWriter, r *http.Request) {
	// Read body
	var err error
	reqBodyReader, err := getRequestBodyReader(r)
	if err != nil {
		common.HttpErrorResponse(err.(common.Error), w)
	}
	body, err := ioutil.ReadAll(reqBodyReader)
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
	// Parse id(s) and get time series from registry
	ids := strings.Split(params["id"], common.IDSeparator)
	submitErr := api.c.Submit(r.Context(), senmlPack, ids)
	if submitErr != nil {
		common.HttpErrorResponse(submitErr, w)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
	return
}

// SubmitWithoutID is a handler for submitting a new data point
// Expected parameters: none
func (api *API) SubmitWithoutID(w http.ResponseWriter, r *http.Request) {

	// Read body
	var err error
	reqBodyReader, err := getRequestBodyReader(r)
	if err != nil {
		common.HttpErrorResponse(err.(common.Error), w)
	}
	body, err := ioutil.ReadAll(reqBodyReader)
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

	submitErr := api.c.Submit(r.Context(), senmlPack, nil)
	if submitErr != nil {
		common.HttpErrorResponse(submitErr, w)
	} else {
		w.Header().Set("Content-Type", common.DefaultMIMEType)
		w.WriteHeader(http.StatusNoContent)
	}
	return
}

func (api *API) Delete(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	r.Context()
	params := mux.Vars(r)

	// Parse id(s) and get sources from registry
	seriesNames := strings.Split(params["id"], common.IDSeparator)

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
	commonErr := api.c.Delete(r.Context(), seriesNames, from, to)
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

func getFormFromQuery(q Query) (form url.Values) {
	form = url.Values{}
	if q.SortAsc {
		form.Set(common.ParamSort, common.Asc)
	}
	if !q.From.IsZero() {
		form.Set(common.ParamFrom, q.From.UTC().Format(time.RFC3339))
	}
	if !q.To.IsZero() {
		form.Set(common.ParamTo, q.To.UTC().Format(time.RFC3339))
	}
	if q.Page > 0 {
		form.Set(common.ParamPage, strconv.Itoa(q.Page))
	}
	if q.PerPage > 0 {
		form.Set(common.ParamPerPage, strconv.Itoa(q.PerPage))
	}
	if q.Count == true {
		form.Set(common.ParamCount, strconv.FormatBool(q.Count))
	}
	if q.Denormalize != 0 {
		if q.Denormalize&DenormMaskTime != 0 {
			form.Add(common.ParamDenormalize, TimeFieldShort)
		}
		if q.Denormalize&DenormMaskName != 0 {
			form.Add(common.ParamDenormalize, NameFieldShort)
		}
		if q.Denormalize&DenormMaskUnit != 0 {
			form.Add(common.ParamDenormalize, UnitFieldShort)
		}
		if q.Denormalize&DenormMaskSum != 0 {
			form.Add(common.ParamDenormalize, SumFieldShort)
		}
		if q.Denormalize&DenormMaskValue != 0 {
			form.Add(common.ParamDenormalize, ValueFieldShort)
		}
	}

	if q.AggrFunc != "" {
		form.Set(common.ParamAggr, q.AggrFunc)
		form.Set(common.ParamWindow, q.AggrWindow.String())
	}

	return form
}

// Utility functions

func ParseQueryParameters(form url.Values) (Query, common.Error) {
	q := Query{}
	var err error

	// start time
	q.From, err = parseFromValue(form.Get(common.ParamFrom))
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
	denormStrings := form[common.ParamDenormalize]
	q.Denormalize, err = parseDenormParams(denormStrings)
	if err != nil {
		return Query{}, &common.BadRequestError{S: fmt.Sprintf("error in param %s: %v", common.ParamDenormalize, err)}
	}

	//get Count
	if strings.EqualFold(form.Get(common.ParamCount), "true") {
		q.Count = true
	}

	//get aggregation parameters
	q.AggrFunc, q.AggrWindow, err = parseAggregationParams(form.Get(common.ParamAggr), form.Get(common.ParamWindow))
	if err != nil {
		return Query{}, &common.BadRequestError{S: fmt.Sprintf("error parsing aggregation params: %v", err)}
	}
	return q, nil
}
