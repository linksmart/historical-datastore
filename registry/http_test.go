package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

func TestHttpIndex(t *testing.T) {
	regAPI := NewRegistryAPI()
	ts := httptest.NewServer(http.HandlerFunc(regAPI.Index))
	defer ts.Close()

	res, err := http.Get(ts.URL)
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

	t.Skip("TODO: test registry body")
}

func TestHttpCreate(t *testing.T) {
	regAPI := NewRegistryAPI()
	ts := httptest.NewServer(http.HandlerFunc(regAPI.Create))
	defer ts.Close()

	b := []byte(`
		{
			"resource": "http://any-domain:8080/1234",
			"meta": {},
			"retention": {
			    "policy": "any_policy",
			    "duration": "any_duration"
			},
			"aggregation": [],
			"type": "any_type",
			"format": "any_format"
		}
		`)

	// try html - should be not supported
	res, err := http.Post(ts.URL+"/registry", "text/plain", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusUnsupportedMediaType {
		t.Errorf("Server response is not %v but %v", http.StatusUnsupportedMediaType, res.StatusCode)
	}

	// try bad payload
	res, err = http.Post(ts.URL+"/registry", "application/json", bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}

	// try a good one
	res, err = http.Post(ts.URL+"/registry", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusCreated {
		t.Errorf("Server response is not %v but %v", http.StatusCreated, res.StatusCode)
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(body))
}

func setupRouter() *mux.Router {
	regAPI := NewRegistryAPI()

	r := mux.NewRouter().StrictSlash(true)
	r.Methods("POST").Path("/registry").HandlerFunc(regAPI.Create)
	r.Methods("GET").Path("/registry/{id}").HandlerFunc(regAPI.Retrieve)
	return r
}

// Create a data source and retrieve it back
func TestHttpRetrieve(t *testing.T) {

	ts := httptest.NewServer(setupRouter())
	defer ts.Close()

	b := []byte(`
		{
			"resource": "http://any-domain:8080/1234",
			"meta": {},
			"retention": {
			    "policy": "any_policy",
			    "duration": "any_duration"
			},
			"aggregation": [],
			"type": "any_type",
			"format": "any_format"
		}
		`)

	// first, create a data source
	res, err := http.Post(ts.URL+"/registry", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}

	// Get response's url (including new uuid)
	url, err := res.Location()
	t.Log("[GET] " + ts.URL + url.Path)

	// Retrieve what it was created
	res, err = http.Get(ts.URL + url.Path)
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}

	// get id from url
	uuid := strings.Split(url.Path, "/")[2]

	// Add generated uuid to the data source
	var ds DataSource
	unmarshalDataSource(body, &ds)
	ds.ID = uuid
	ds.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, ds.ID)
	ds.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, ds.ID)
	get_b, _ := json.Marshal(ds)

	// compare created data with the source
	if string(body) != string(get_b) {
		t.Errorf("Unexpected response: %s", string(body))
	}
}

func TestHttpUpdate(t *testing.T) {
	t.Skip("TODO: API handler test")
}

func TestHttpDelete(t *testing.T) {
	t.Skip("TODO: API handler test")
}
