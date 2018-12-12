// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package data

import (
	"bytes"
	"code.linksmart.eu/hds/historical-datastore/common"
	"encoding/json"
	"fmt"
	"github.com/cisco/senml"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/gorilla/mux"
)

func setupHTTPAPI() (*mux.Router, []string) {
	regStorage := registry.NewMemoryStorage(common.RegConf{})

	// Create three dummy datasources with different types
	var testIDs []string
	dss := []registry.DataSource{
		{
			Resource: "http://example.com/sensor1",
			Type:     "float",
		},
		{
			Resource: "http://example.com/sensor2",
			Type:     "bool",
		},
		{
			Resource: "http://example.com/sensor3",
			Type:     "string",
		},
	}
	for _, ds := range dss {
		created, err := regStorage.Add(ds)
		if err != nil {
			fmt.Println("Error creating dummy DS:", err)
			break
		}
		testIDs = append(testIDs, created.ID)
	}

	api := NewAPI(regStorage, &dummyDataStorage{}, false)

	r := mux.NewRouter().StrictSlash(true)
	r.Methods("POST").Path("/data/{id}").HandlerFunc(api.Submit)
	r.Methods("GET").Path("/data/{id}").HandlerFunc(api.Query)
	return r, testIDs
}

func TestHttpSubmit(t *testing.T) {
	router, testIDs := setupHTTPAPI()
	ts := httptest.NewServer(router)
	defer ts.Close()

	v1 := 42.0
	r1 := senml.SenMLRecord{
		Name:  "sensor1",
		Unit:  "degC",
		Value: &v1,
	}
	v2 := true
	r2 := senml.SenMLRecord{
		Name:      "sensor2",
		Unit:      "flag",
		BoolValue: &v2,
	}
	v3 := "test string"
	r3 := senml.SenMLRecord{
		Name:        "sensor3",
		Unit:        "char",
		StringValue: v3,
	}

	r1.BaseName = "http://example.com/"
	records := []senml.SenMLRecord{r1, r2, r3}

	b, _ := json.Marshal(records)

	all := strings.Join(testIDs, ",")
	// try html - should be not supported
	res, err := http.Post(ts.URL+"/data/"+all, "application/text+html", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	//if res.StatusCode != http.StatusUnsupportedMediaType {
	//	t.Errorf("Server response is not %v but %v", http.StatusUnsupportedMediaType, res.StatusCode)
	//}

	// try bad payload
	res, err = http.Post(ts.URL+"/data/"+all, "application/senml+json", bytes.NewReader([]byte{0xde, 0xad}))
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

// DUMMY DATA STORAGE

type dummyDataStorage struct{}

func (s *dummyDataStorage) Submit(data map[string][]senml.SenMLRecord, sources map[string]*registry.DataSource) error {
	return nil
}
func (s *dummyDataStorage) Query(q Query, page, perPage int, ds ...*registry.DataSource) (senml.SenML, int, error) {
	return senml.SenML{}, 0, nil
}
func (s *dummyDataStorage) CreateHandler(ds registry.DataSource) error {
	return nil
}
func (s *dummyDataStorage) UpdateHandler(old registry.DataSource, new registry.DataSource) error {
	return nil
}
func (s *dummyDataStorage) DeleteHandler(ds registry.DataSource) error {
	return nil
}
