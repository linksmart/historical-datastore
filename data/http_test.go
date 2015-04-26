package data

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	senml "linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/krylovsk/gosenml"
	"linksmart.eu/services/historical-datastore/registry"
)

type dummyDataStorage struct{}

// data is a map where keys are data source ids
func (s *dummyDataStorage) submit(data map[string][]DataPoint, sources map[string]registry.DataSource) error {
	return nil
}

// Retrieves last data point of every data source
func (s *dummyDataStorage) getLast(ds ...registry.DataSource) (DataSet, error) {
	return DataSet{}, nil
}

func (s *dummyDataStorage) query(q query, page, perPage int, ds ...registry.DataSource) (DataSet, int, error) {
	return DataSet{}, 0, nil
}

func setupAPI() *DataAPI {
	registryClient := registry.NewLocalClient(&registry.DummyRegistryStorage{})
	return NewDataAPI(registryClient, &dummyDataStorage{})
}

// func setupAPI() *DataAPI {
// 	u, _ := url.Parse("http://localhost:8086")
// 	storageCfg := InfluxStorageConfig{
// 		URL:      *u,
// 		Database: "test",
// 	}
// 	storage, _ := NewInfluxStorage(&storageCfg)
// 	registryClient := registry.NewLocalClient(&registry.DummyRegistryStorage{})

// 	return NewDataAPI(registryClient, storage)
// }

func setupRouter() *mux.Router {
	api := setupAPI()

	r := mux.NewRouter().StrictSlash(true)
	r.Methods("POST").Path("/data/{id}").HandlerFunc(api.Submit)
	r.Methods("GET").Path("/data/{id}").HandlerFunc(api.Query)
	return r
}

func TestHttpSubmit(t *testing.T) {
	ts := httptest.NewServer(setupRouter())
	defer ts.Close()

	v1 := 42.0
	e1 := senml.Entry{
		Name:  "sensor1",
		Units: "degC",
		Value: &v1,
	}
	v2 := true
	e2 := senml.Entry{
		Name:         "sensor2",
		Units:        "flag",
		BooleanValue: &v2,
	}
	v3 := "test string"
	e3 := senml.Entry{
		Name:        "sensor3",
		Units:       "char",
		StringValue: &v3,
	}

	m := senml.NewMessage(e1, e2, e3)
	m.BaseName = "http://example.com/"

	encoder := senml.NewJSONEncoder()
	b, _ := encoder.EncodeMessage(m)

	// try html - should be not supported
	res, err := http.Post(ts.URL+"/data/12345,67890,1337", "application/text+html", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("Server response is not %v but %v", http.StatusUnsupportedMediaType, res.StatusCode)
	}

	// try bad payload
	res, err = http.Post(ts.URL+"/data/12345,67890,1337", "application/senml+json", bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}

	// try a good one
	res, err = http.Post(ts.URL+"/data/12345,67890,1337", "application/senml+json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusAccepted {
		t.Errorf("Server response is not %v but %v", http.StatusAccepted, res.StatusCode)
	}
}

func TestHttpQuery(t *testing.T) {
	ts := httptest.NewServer(setupRouter())
	defer ts.Close()

	res, err := http.Get(ts.URL + "/data/12345,67890,1337?limit=3&start=2015-04-24T11:56:51Z&page=1&per_page=10")
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("Server response is not %v but %v", http.StatusOK, res.StatusCode)
	}

	_, err = ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	// TODO
	// t.Error("TODO: check response body")
}
