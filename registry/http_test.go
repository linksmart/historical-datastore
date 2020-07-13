// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/linksmart/historical-datastore/common"
)

func setupRouter(regAPI *API) *mux.Router {
	r := mux.NewRouter().StrictSlash(true).SkipClean(true)
	r.Methods("GET").Path("/registry").HandlerFunc(regAPI.Index)
	r.Methods("POST").Path("/registry").HandlerFunc(regAPI.Create)
	r.Methods("GET").Path("/registry/{type}/{path}/{op}/{value:.*}").HandlerFunc(regAPI.Filter)
	r.Methods("GET").Path("/registry/{id:.+}").HandlerFunc(regAPI.Retrieve)
	r.Methods("PUT").Path("/registry/{id:.+}").HandlerFunc(regAPI.Update)
	r.Methods("DELETE").Path("/registry/{id:.+}").HandlerFunc(regAPI.Delete)

	return r
}

func setupAPI() (*API, Storage) {
	regStorage := setupMemStorage()
	regAPI := NewAPI(regStorage)

	return regAPI, regStorage
}

// Manually send an HTTP request and get the response
func httpRequestClient(method string, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	//req.Header.Set("Content-Type", "")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func TestHttpIndex(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create some dummy data
	totalDummy := 55
	generateDummyData(totalDummy, registryClient)

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// Get the registry with default query parameters
	res, err := http.Get(ts.URL + common.RegistryAPILoc)
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
	var reg TimeSeriesList
	err = json.Unmarshal(body, &reg)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// Compare total created with total variable in returned registry
	if reg.Total != totalDummy {
		t.Errorf("Mismatched total created(%d) and accounted(%d) time series!", totalDummy, reg.Total)
	}

	//// Now, check body of the registry for each page

	// Compare created and returned time series
	totalReturnedTS := 0
	perPage := 10
	pages := int(math.Ceil(float64(totalDummy) / float64(perPage)))
	for page := 1; page <= pages; page++ {
		// Get the specific page
		res, err := http.Get(fmt.Sprintf("%s%s?page=%d&perPage=%d", ts.URL, common.RegistryAPILoc, page, perPage))
		if err != nil {
			t.Fatalf(err.Error())
		}

		// Get the body and unmarshal it
		body, err := ioutil.ReadAll(res.Body)
		defer res.Body.Close()
		if err != nil {
			t.Fatalf(err.Error())
		}
		var reg TimeSeriesList
		err = json.Unmarshal(body, &reg)
		if err != nil {
			t.Fatalf(err.Error())
		}

		// QueryPage the local data for comparison
		dummyTSs, _, _ := registryClient.GetMany(page, perPage)

		// Number of expected items in this page
		inThisPage := len(dummyTSs)
		//		inThisPage := perPage
		//		if (totalDummy - (page-1)*perPage) < perPage {
		//			inThisPage = int(math.Mod(float64(totalDummy), float64(perPage)))
		//		}

		// Check for each time series in this page
		for i := 0; i < inThisPage; i++ {
			dummyTS := dummyTSs[i]
			returnedTS := reg.Series[i]

			// compare them
			dummyTS_b, _ := json.Marshal(dummyTS)
			returnedTS_b, _ := json.Marshal(returnedTS)
			if string(dummyTS_b) != string(returnedTS_b) {
				t.Errorf("Mismatch retrieved:\n%s\n and stored:\n%s\n", string(dummyTS_b), string(returnedTS_b))
			}
		}

		totalReturnedTS += len(reg.Series)
	}

	// Compare the total number of created and retrieved(in all pages of registry) time series
	if totalReturnedTS != totalDummy {
		t.Errorf("Mismatched total created(%d) and returned(%d) time series!", totalDummy, totalReturnedTS)
	}

	return
}

func TestHttpCreate(t *testing.T) {
	regAPI, registryClient := setupAPI()
	MIMEType := "unknown/unknown"

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// try bad payload
	res, err := http.Post(ts.URL+common.RegistryAPILoc, MIMEType, bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}
	res.Body.Close()

	// try invalid bodies
	for _, invalidBodyStr := range append(invalidBodies, invalidPostBodies...) {
		invalidBody := []byte(invalidBodyStr)

		res, err := http.Post(ts.URL+common.RegistryAPILoc, MIMEType, bytes.NewReader(invalidBody))
		if err != nil {
			t.Errorf(err.Error())
		}
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("Server response is not %v but %v :\n%v", http.StatusBadRequest, res.StatusCode, invalidBodyStr)
		}
		res.Body.Close()
	}

	// try a good one
	b := []byte(`
		{
			"name": "any_url",
			"datatype": "string"
		}
		`)
	res, err = http.Post(ts.URL+common.RegistryAPILoc, MIMEType, bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("Server response is not %v but %v", http.StatusCreated, res.StatusCode)
	}
	// Get response's url (including new uuid)
	url, err := res.Location()
	if err != nil {
		t.Fatalf(err.Error())
	}
	res.Body.Close()

	// Extract id of new time series from url
	splitURL := strings.Split(url.Path, "/")
	if len(splitURL) != 3 {
		t.Fatal("Invalid url in Location header-entry")
	}
	name := splitURL[2]

	// Manually construct the expected POSTed time series
	var postedTS TimeSeries
	err = json.Unmarshal(b, &postedTS)
	if err != nil {
		t.Fatalf(err.Error())
	}
	postedTS.Name = name
	//postedTS.Name = fmt.Sprintf("%s/%s", common.DataAPILoc, postedTS.Name)

	// Retrieve the added time series
	addedTS, _ := registryClient.Get(name)

	// marshal the stored time series for comparison
	postedTS_b, _ := json.Marshal(&postedTS)
	addedTS_b, _ := json.Marshal(&addedTS)

	// compare updated(PUT) time series with the one in memory
	if string(postedTS_b) != string(addedTS_b) {
		t.Errorf("The POSTed data:\n%s\n mismatch the stored data:\n%s\n", string(postedTS_b), string(addedTS_b))
	}

	return
}

// Create a time series and retrieve it back
func TestHttpRetrieve(t *testing.T) {
	regAPI, registryClient := setupAPI()

	IDs, err := generateDummyData(1, registryClient)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ID := IDs[0]

	aDataSource, _ := registryClient.Get(ID)

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// Retrieve what it was created
	res, err := http.Get(fmt.Sprintf("%v%s/%s", ts.URL, common.RegistryAPILoc, ID))
	if err != nil {
		t.Fatalf(err.Error())
	}
	b, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}

	// marshal the stored time series for comparison
	storedTS_b, _ := json.Marshal(&aDataSource)

	// compare stored and retrieved(GET) time series
	if string(storedTS_b) != string(b) {
		t.Errorf("Retrieved(GET):\n%s\n mismatch the stored data:\n%s\n", string(b), string(storedTS_b))
	}
}

func TestHttpUpdate(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create a dummy time series
	names, err := generateDummyData(1, registryClient)
	if err != nil {
		t.Fatalf(err.Error())
	}
	name := names[0]

	testServer := httptest.NewServer(setupRouter(regAPI))
	defer testServer.Close()

	url := fmt.Sprintf("%s%s/%s", testServer.URL, common.RegistryAPILoc, name)

	// try bad payload
	res, err := httpRequestClient("PUT", url, bytes.NewReader([]byte{0xde, 0xad}))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("Server response is not %v but %v", http.StatusBadRequest, res.StatusCode)
	}
	res.Body.Close()

	// try invalid bodies
	for _, invalidBodyStr := range append(invalidBodies, invalidPutBodies...) {
		invalidBody := []byte(invalidBodyStr)

		res, err := httpRequestClient("PUT", url, bytes.NewReader(invalidBody))
		if err != nil {
			t.Fatalf(err.Error())
		}
		if res.StatusCode != http.StatusConflict {
			t.Fatalf("Server response is not %v but %v :\n%v", http.StatusConflict, res.StatusCode, invalidBodyStr)
		}
		res.Body.Close()
		break
	}

	// Retrieve the stored ts
	ts, err := registryClient.Get(name)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ts.Retention.Min = "3h"
	b, err := json.Marshal(&ts)
	if err != nil {
		t.Fatalf(err.Error())
	}

	res, err = httpRequestClient("PUT", url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("Server response is not %v but %v", http.StatusOK, res.StatusCode)
	}
	res.Body.Close()

	// Retrieve the updated time series
	updatedTS, _ := registryClient.Get(name)
	updated_b, _ := json.Marshal(&updatedTS)

	// compare updated(PUT) time series with the one in memory
	if string(b) != string(updated_b) {
		t.Errorf("The submitted PUT:\n%s\n mismatch the stored data:\n%s\n", string(b), string(updated_b))
	}
}

func TestHttpDelete(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create a dummy time series
	names, err := generateDummyData(1, registryClient)
	if err != nil {
		t.Fatalf(err.Error())
	}
	name := names[0]

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// Try deleting an existing item
	url := fmt.Sprintf("%s%s/%s", ts.URL, common.RegistryAPILoc, name)
	res, err := httpRequestClient("DELETE", url, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Server response is %v instead of %v", res.StatusCode, http.StatusOK)
	}
	// check whether it is deleted
	_, err = registryClient.Get(name)
	if err == nil {
		t.Fatalf("Server responded %v but time series is not deleted!", res.StatusCode)
	}

	// Try deleting a non-existing item
	url = ts.URL + "/registry/" + "f5e0a314-0c8c-4938-9961-74625c6614da"
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("Server response is %v instead of %v", res.StatusCode, http.StatusNotFound)
	}

}

func TestHttpFilter(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create some dummy data
	dummyTSs := []TimeSeries{
		TimeSeries{
			Name: "dimmer.eu/sensor1",
			Type: String,
		},
		TimeSeries{
			Name: "dimmer.eu/sensor2",
			Type: Bool,
		},
		TimeSeries{
			Name: "dimmer.eu/actuator1",
			Type: String,
		},
	}
	for _, ts := range dummyTSs {
		registryClient.Add(ts)
	}

	testServer := httptest.NewServer(setupRouter(regAPI))
	defer testServer.Close()

	// A function to generate filter url
	filterURL := func(filterStr string) string {
		// /registry/{path}/{type}/{op}/{value}
		return fmt.Sprintf("%v%s/%s", testServer.URL, common.RegistryAPILoc, filterStr)
	}

	// Search for the time series with Type: bool
	res, err := http.Get(filterURL(FTypeOne + "/dataType/equals/bool"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Server response is %v instead of %v", res.StatusCode, http.StatusOK)
	}
	b, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Check if it was queried correctly
	var reg TimeSeriesList
	err = json.Unmarshal(b, &reg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(reg.Series) != 1 {
		t.Errorf("Instead of one, it returned %d time series.", len(reg.Series))
	}
	if reg.Series[0].Type != Bool {
		t.Errorf("Instead of the expected datasource (Type:bool), it returned:\n%+v", reg.Series[0])
	}

	// Search for time series that contains "sensor" in Resource
	res, err = http.Get(filterURL(FTypeMany + "/name/contains/dimmer.eu/sensor"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	b, err = ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = json.Unmarshal(b, &reg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Check if the total is correct
	if reg.Total != 2 || len(reg.Series) != 2 {
		t.Errorf("Catalog contains total %d(%d entries) instead of 2 time series:\n %+v", reg.Total, len(reg.Series), reg)
	}
	// Check if correct entries are queried
	for _, ts := range reg.Series {
		if !strings.Contains(ts.Name, "sensor") {
			t.Errorf("Catalog entry resource contains something other than 'sensor': %+v", ts.Name)
		}
	}
}

// A pool of bad time series
var (
	invalidBodies = []string{
		// Empty name //////////
		`{
			"name": "",
			"dataType": "string"
		}`,
		// Invalid name //////////
		`{
			"name": "#3",
			"dataType": "string"
		}`,
		// Invalid type //////////
		`{
			"name": "any_url",
			"dataType": "some_unsupportedType"
		}`,
	}

	invalidPostBodies = []string{
		// Missing resource url //////////
		`{
			"name": "",
			"dataType": "string"
		}`,
		// Missing type //////////
		`{
			"name": "any_url",
			"dataType": ""
		}`,
		// Invalid type //////////
		`{
			"name": "any_url",
			"dataType": "some_unsupported_type"
		}`,
	}

	invalidPutBodies = []string{
		// Provided read-only resource url //////////
		`{
			"name": "any_url"
		}`,
		// Provided read-only type //////////
		`{
			"dataType": "string"
		}`,
	}
)
