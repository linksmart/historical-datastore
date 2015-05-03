package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

func setupRouter() *mux.Router {
	regStorage := NewMemoryStorage()
	regAPI := NewRegistryAPI(regStorage)

	r := mux.NewRouter().StrictSlash(true)
	r.Methods("GET").Path("/registry").HandlerFunc(regAPI.Index)
	r.Methods("POST").Path("/registry").HandlerFunc(regAPI.Create)
	r.Methods("GET").Path("/registry/{id}").HandlerFunc(regAPI.Retrieve)
	r.Methods("PUT").Path("/registry/{id}").HandlerFunc(regAPI.Update)
	r.Methods("DELETE").Path("/registry/{id}").HandlerFunc(regAPI.Delete)
	r.Methods("GET").Path("/registry/{path}/{type}/{op}/{value}").HandlerFunc(regAPI.Filter)
	return r
}

func TestHttpIndex(t *testing.T) {
	// for some reason, setupRouter() doesn't work on Index
	//	ts := httptest.NewServer(setupRouter())
	//	defer ts.Close()
	regStorage := NewMemoryStorage()
	regAPI := NewRegistryAPI(regStorage)
	registryClient := NewLocalClient(regStorage)

	// Create some dummy data
	totalDummy := 555
	GenerateDummyData(totalDummy, registryClient)

	ts := httptest.NewServer(http.HandlerFunc(regAPI.Index))
	defer ts.Close()

	// Get the registry with default query parameters
	res, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if res.StatusCode != http.StatusOK {
		t.Fatalf("Server response is not %v but %v", http.StatusOK, res.StatusCode)
	}

	// Get the body and unmarshal it
	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	var reg Registry
	err = json.Unmarshal(body, &reg)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Compare total created with total variable in returned registry
	if reg.Total != totalDummy {
		t.Errorf("Mismatched total created(%d) and accounted(%d) data sources!", totalDummy, reg.Total)
	}

	//// Now, check body of the registry for each page

	// Compare created and returned data sources
	var returnedDSs []DataSource
	perPage := 100
	pages := int(math.Ceil(float64(totalDummy) / float64(perPage)))
	for page := 1; page <= pages; page++ {
		// Get the specific page
		res, err := http.Get(fmt.Sprintf("%s?page=%d&per_page=%d", ts.URL, page, perPage))
		if err != nil {
			t.Fatalf(err.Error())
		}

		// Get the body and unmarshal it
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			t.Fatalf(err.Error())
		}
		var reg Registry
		err = json.Unmarshal(body, &reg)
		if err != nil {
			t.Fatalf(err.Error())
		}

		// Query the local data for comparison
		dummyDSs, _, _ := registryClient.GetDataSources(page, perPage)

		// Number of expected items in this page
		inThisPage := len(dummyDSs)
		//		inThisPage := perPage
		//		if (totalDummy - (page-1)*perPage) < perPage {
		//			inThisPage = int(math.Mod(float64(totalDummy), float64(perPage)))
		//		}

		// Check for each data source in this page
		for i := 0; i < inThisPage; i++ {
			dummyDS := dummyDSs[i]
			returnedDS := reg.Entries[i]
			if dummyDS.ID != returnedDS.ID {
				t.Errorf("Mismatched datasource: \n%v \n%v\n", dummyDSs[i], reg.Entries[i])
			}
		}

		returnedDSs = append(returnedDSs, reg.Entries...)
	}

	// Compare total created with total datasources in all pages of registry
	if len(returnedDSs) != totalDummy {
		t.Errorf("Mismatched total created(%d) and returned(%d) data sources!", totalDummy, len(returnedDSs))
	}

	return
}

func TestHttpCreate(t *testing.T) {
	ts := httptest.NewServer(setupRouter())
	defer ts.Close()

	b := []byte(`
		{
			"resource": "http://any-domain:8080/1234",
			"meta": {},
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"aggregation": [],
			"type": "string",
			"format": "any_format"
		}
		`)

	// try bad payload
	res, err := http.Post(ts.URL+"/registry", "unknown/unknown", bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Errorf(err.Error())
	}

	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}

	// try a good one
	res, err = http.Post(ts.URL+"/registry", "unknown/unknown", bytes.NewReader(b))
	if err != nil {
		t.Errorf(err.Error())
	}

	if res.StatusCode != http.StatusCreated {
		t.Errorf("Server response is not %v but %v", http.StatusCreated, res.StatusCode)
	}

	//	body, err := ioutil.ReadAll(res.Body)
	//	defer res.Body.Close()
	//	if err != nil {
	//		t.Errorf(err.Error())
	//	}
	//t.Log(string(body))
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
			    "policy": "1w",
			    "duration": "1m"
			},
			"aggregation": [],
			"type": "string",
			"format": "any_format"
		}
		`)

	// first, create a data source
	res, err := http.Post(ts.URL+"/registry", "unknown/unknown", bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Get response's url (including new uuid)
	url, err := res.Location()
	t.Log("[GET] " + ts.URL + url.Path)

	// Retrieve what it was created
	res, err = http.Get(ts.URL + url.Path)
	if err != nil {
		t.Fatalf(err.Error())
	}

	body, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}

	// get id from url
	uuid := strings.Split(url.Path, "/")[2]

	// Add generated uuid to the data source
	var ds DataSource
	err = json.Unmarshal(body, &ds)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ds.ID = uuid
	ds.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, ds.ID)
	ds.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, ds.ID)
	get_b, err := json.Marshal(&ds)
	if err != nil {
		t.Fatalf(err.Error())
	}

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

// Generate dummy data sources
func GenerateDummyData(quantity int, c *LocalClient) {
	rand.Seed(time.Now().UTC().UnixNano())

	fmt.Printf(">>> NOTE: GENERATING %d DUMMY DATASOURCES <<<\n", quantity)
	for i := 1; i <= quantity; i++ {
		var ds DataSource
		ds.Resource = fmt.Sprintf("http://example.com/sensor%d", i)
		ds.Meta = make(map[string]interface{})
		ds.Meta["SerialNumber"] = randInt(10000, 99999)
		ds.Retention.Policy = fmt.Sprintf("%d%s", randInt(1, 20), common.RetentionPeriods()[randInt(0, 3)])
		ds.Retention.Duration = fmt.Sprintf("%d%s", randInt(1, 20), common.RetentionPeriods()[randInt(0, 3)])
		//ds.Aggregation TODO
		ds.Type = common.SupportedTypes()[randInt(0, 2)]
		ds.Format = "application/senml+json"

		c.storage.add(&ds)
	}
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
