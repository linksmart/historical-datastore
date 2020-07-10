// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"

	"github.com/gorilla/mux"
	"github.com/linksmart/historical-datastore/registry"
)

func setupHTTPAPI() (*mux.Router, []string) {
	regStorage := registry.NewMemoryStorage(common.RegConf{})

	// Create three dummy datastreams with different types
	var testIDs []string
	dss := []registry.DataStream{
		{
			Name: "http://example.com/sensor1",
			Unit: "degC",
			Type: registry.Float,
		},
		{
			Name: "http://example.com/sensor2",
			Unit: "flag",
			Type: registry.Bool,
		},
		{
			Name: "http://example.com/sensor3",
			Unit: "char",
			Type: registry.String,
		},
	}
	for _, ds := range dss {
		created, err := regStorage.Add(ds)
		if err != nil {
			fmt.Println("Error creating dummy DS:", err)
			break
		}
		testIDs = append(testIDs, created.Name)
	}

	api := NewAPI(regStorage, &dummyDataStorage{}, false)

	r := mux.NewRouter().StrictSlash(true).SkipClean(true)
	r.Methods("POST").Path("/data/{id:.+}").HandlerFunc(api.Submit)
	r.Methods("GET").Path("/data/{id:.+}").HandlerFunc(api.Query)
	r.Methods("DELETE").Path("/data/{id:.+}").HandlerFunc(api.Delete)
	return r, testIDs
}

func TestHttpSubmit(t *testing.T) {
	router, testIDs := setupHTTPAPI()
	ts := httptest.NewServer(router)
	defer ts.Close()

	v1 := 42.0
	r1 := senml.Record{
		Name:  "example.com/sensor1",
		Unit:  "degC",
		Value: &v1,
	}
	v2 := true
	r2 := senml.Record{
		Name:      "example.com/sensor2",
		Unit:      "flag",
		BoolValue: &v2,
	}
	v3 := "test string"
	r3 := senml.Record{
		Name:        "example.com/sensor3",
		Unit:        "char",
		StringValue: v3,
	}

	r1.BaseName = "http://"
	records := []senml.Record{r1, r2, r3}

	b, _ := json.Marshal(records)

	all := strings.Join(testIDs, ",")
	//all = strings.Replace(all,":","",-1)
	//all = strings.Replace(all, "/", "", -1)

	//try empty payload
	res, err := http.Post(ts.URL+"/data/"+all, "", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusAccepted {
		t.Errorf("Server response is not %v but %v", http.StatusAccepted, res.StatusCode)
	}

	// try html - should be not supported
	res, err = http.Post(ts.URL+"/data/"+all, "application/text+html", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("Server response is not %v but %v", http.StatusUnsupportedMediaType, res.StatusCode)
	}

	// try bad payload
	res, err = http.Post(ts.URL+"/data/"+all, "application/senml+json", bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}

	//when pack is has a datastream not in the URL
	res, err = http.Post(ts.URL+"/data/"+testIDs[0], "application/senml+json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}
	// try a good one
	res, err = http.Post(ts.URL+"/data/"+all, "application/senml+json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusAccepted {
		t.Errorf("Server response is not %v but %v", http.StatusAccepted, res.StatusCode)
	}
}

func TestHttpQuery(t *testing.T) {
	router, testIDs := setupHTTPAPI()
	ts := httptest.NewServer(router)
	defer ts.Close()

	all := strings.Join(testIDs, ",")
	res, err := http.Get(ts.URL + "/data/" + all + "?limit=3&start=2015-04-24T11:56:51Z&page=1&per_page=12")
	if err != nil {
		t.Fatal(err)
	}

	b, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("Server response is not %v but %v. \nResponse body:%s", http.StatusOK, res.StatusCode, string(b))
	}

	//TODO
	//t.Error("TODO: check response body")
}

func TestAPI_Delete(t *testing.T) {
	router, testIDs := setupHTTPAPI()
	ts := httptest.NewServer(router)
	defer ts.Close()

	all := strings.Join(testIDs, ",")
	res, err := httpDoRequest(http.MethodDelete, ts.URL+"/data/"+all+"?from=2015-04-24T11:56:51Z", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("Server should return %v, got instead: %d", http.StatusNoContent, res.StatusCode)
	}
}

//TODO TEST limits

// DUMMY DATA STORAGE

type dummyDataStorage struct{}

func (s *dummyDataStorage) Submit(data map[string]senml.Pack, streams map[string]*registry.DataStream) error {
	return nil
}
func (s *dummyDataStorage) QueryPage(q Query, streams ...*registry.DataStream) (pack senml.Pack, total *int, err error) {
	return senml.Pack{}, nil, nil
}

func (s *dummyDataStorage) QueryStream(q Query, sendFunc sendFunction, streams ...*registry.DataStream) error {
	return nil
}

func (s *dummyDataStorage) Count(q Query, streams ...*registry.DataStream) (total int, err error) {
	return 0, nil
}

func (s *dummyDataStorage) Delete(streams []*registry.DataStream, from time.Time, to time.Time) (err error) {
	return nil
}
func (s *dummyDataStorage) Disconnect() error {
	return nil
}
func (s *dummyDataStorage) CreateHandler(ds registry.DataStream) error {
	return nil
}
func (s *dummyDataStorage) UpdateHandler(old registry.DataStream, new registry.DataStream) error {
	return nil
}
func (s *dummyDataStorage) DeleteHandler(ds registry.DataStream) error {
	return nil
}

func httpDoRequest(method, url string, r *bytes.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}
