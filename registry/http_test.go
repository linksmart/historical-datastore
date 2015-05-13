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

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

func setupRouter(regAPI *WriteableAPI) *mux.Router {
	r := mux.NewRouter().StrictSlash(true)
	r.Methods("GET").Path("/registry").HandlerFunc(regAPI.Index)
	r.Methods("POST").Path("/registry").HandlerFunc(regAPI.Create)
	r.Methods("GET").Path("/registry/{id}").HandlerFunc(regAPI.Retrieve)
	r.Methods("PUT").Path("/registry/{id}").HandlerFunc(regAPI.Update)
	r.Methods("DELETE").Path("/registry/{id}").HandlerFunc(regAPI.Delete)
	r.Methods("GET").Path("/registry/{type}/{path}/{op}/{value:.*}").HandlerFunc(regAPI.Filter)
	return r
}

func setupAPI() (*WriteableAPI, Client) {
	// Setup and run the notifier
	ntSndRegCh := make(chan common.Notification)
	ntRcvDataCh := make(chan common.Notification)
	// nrAggrCh := make(chan int)
	common.NewNotifier(ntSndRegCh, ntRcvDataCh)
	regStorage := NewMemoryStorage()
	regAPI := NewWriteableAPI(regStorage, ntSndRegCh)
	registryClient := NewLocalClient(regStorage)

	return regAPI, registryClient
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
	totalDummy := 555
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
	totalReturnedDS := 0
	perPage := 100
	pages := int(math.Ceil(float64(totalDummy) / float64(perPage)))
	for page := 1; page <= pages; page++ {
		// Get the specific page
		res, err := http.Get(fmt.Sprintf("%s%s?page=%d&per_page=%d", ts.URL, common.RegistryAPILoc, page, perPage))
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

			// compare them
			dummyDS_b, _ := json.Marshal(dummyDS)
			returnedDS_b, _ := json.Marshal(returnedDS)
			if string(dummyDS_b) != string(returnedDS_b) {
				t.Errorf("Mismatch retrieved:\n%s\n and stored:\n%s\n", string(dummyDS_b), string(returnedDS_b))
			}
		}

		totalReturnedDS += len(reg.Entries)
	}

	// Compare the total number of created and retrieved(in all pages of registry) data sources
	if totalReturnedDS != totalDummy {
		t.Errorf("Mismatched total created(%d) and returned(%d) data sources!", totalDummy, totalReturnedDS)
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
			t.Fatalf(err.Error())
		}
		if res.StatusCode != http.StatusConflict {
			t.Fatalf("Server response is not %v but %v :\n%v", http.StatusConflict, res.StatusCode, invalidBodyStr)
		}
		res.Body.Close()
	}

	// try a good one
	b := []byte(`
		{
			"resource": "any_url",
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
	res, err = http.Post(ts.URL+common.RegistryAPILoc, MIMEType, bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusCreated {
		t.Errorf("Server response is not %v but %v", http.StatusCreated, res.StatusCode)
	}
	// Get response's url (including new uuid)
	url, err := res.Location()
	if err != nil {
		t.Fatalf(err.Error())
	}
	res.Body.Close()

	// Extract id of new data source from url
	splitURL := strings.Split(url.Path, "/")
	if len(splitURL) != 3 {
		t.Fatal("Invalid url in Location header-entry")
	}
	id := splitURL[2]

	// Manually construct the expected POSTed data source
	var postedDS DataSource
	err = json.Unmarshal(b, &postedDS)
	if err != nil {
		t.Fatalf(err.Error())
	}
	postedDS.ID = id
	postedDS.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, postedDS.ID)
	postedDS.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, postedDS.ID)

	// Retrieve the added data source
	addedDS, _ := registryClient.Get(id)

	// marshal the stored data source for comparison
	postedDS_b, _ := json.Marshal(&postedDS)
	addedDS_b, _ := json.Marshal(&addedDS)

	// compare updated(PUT) data source with the one in memory
	if string(postedDS_b) != string(addedDS_b) {
		t.Errorf("The POSTed data:\n%s\n mismatch the stored data:\n%s\n", string(postedDS_b), string(addedDS_b))
	}

	return
}

// Create a data source and retrieve it back
func TestHttpRetrieve(t *testing.T) {
	regAPI, registryClient := setupAPI()

	ID := generateDummyData(1, registryClient)[0]
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

	// marshal the stored data source for comparison
	storedDS_b, _ := json.Marshal(&aDataSource)

	// compare stored and retrieved(GET) data sources
	if string(storedDS_b) != string(b) {
		t.Errorf("Retrieved(GET):\n%s\n mismatch the stored data:\n%s\n", string(b), string(storedDS_b))
	}
}

func TestHttpUpdate(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create a dummy data source
	ID := generateDummyData(1, registryClient)[0]

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	url := fmt.Sprintf("%s%s/%s", ts.URL, common.RegistryAPILoc, ID)

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
	}

	// try a good one
	b := []byte(`
		{
			"meta": {},
			"retention": {
			    "policy": "1w",
			    "duration": "1m"
			},
			"aggregation": [],
			"format": "any_format"
		}
		`)
	res, err = httpRequestClient("PUT", url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("Server response is not %v but %v", http.StatusOK, res.StatusCode)
	}
	res.Body.Close()

	// Retrieve the updated data source
	updatedDS, _ := registryClient.Get(ID)

	// Manually construct the expected updated(PUT) data source
	var putDS DataSource
	err = json.Unmarshal(b, &putDS)
	if err != nil {
		t.Fatalf(err.Error())
	}
	putDS.ID = ID
	putDS.URL = fmt.Sprintf("%s/%s", common.RegistryAPILoc, putDS.ID)
	putDS.Data = fmt.Sprintf("%s/%s", common.DataAPILoc, putDS.ID)
	putDS.Resource = updatedDS.Resource
	putDS.Type = updatedDS.Type

	// marshal the stored data source for comparison
	putDS_b, _ := json.Marshal(&putDS)
	updatedDS_b, _ := json.Marshal(&updatedDS)

	// compare updated(PUT) data source with the one in memory
	if string(putDS_b) != string(updatedDS_b) {
		t.Errorf("The submitted PUT:\n%s\n mismatch the stored data:\n%s\n", string(putDS_b), string(updatedDS_b))
	}
}

func TestHttpDelete(t *testing.T) {
	regAPI, registryClient := setupAPI()

	// Create a dummy data source
	ID := generateDummyData(1, registryClient)[0]

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// Try deleting an existing item
	url := fmt.Sprintf("%s%s/%s", ts.URL, common.RegistryAPILoc, ID)
	res, err := httpRequestClient("DELETE", url, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if res.StatusCode != http.StatusOK {
		t.Fatalf("Server response is %v instead of %v", res.StatusCode, http.StatusOK)
	}
	// check whether it is deleted
	_, err = registryClient.Get(ID)
	if err == nil {
		t.Fatalf("Server responded %v but data source is not deleted!", res.StatusCode)
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
	dummyDSs := []DataSource{
		DataSource{
			Resource: "dimmer.eu/sensor1",
			Type:     "string",
			Format:   "application/json",
		},
		DataSource{
			Resource: "dimmer.eu/sensor2",
			Type:     "bool",
			Format:   "application/json",
		},
		DataSource{
			Resource: "dimmer.eu/actuator1",
			Type:     "string",
			Format:   "application/json",
		},
	}
	for _, ds := range dummyDSs {
		registryClient.Add(ds)
	}

	ts := httptest.NewServer(setupRouter(regAPI))
	defer ts.Close()

	// A function to generate filter url
	filterURL := func(filterStr string) string {
		// /registry/{path}/{type}/{op}/{value}
		return fmt.Sprintf("%v%s/%s", ts.URL, common.RegistryAPILoc, filterStr)
	}

	// Search for the data source with Type: bool
	res, err := http.Get(filterURL(FTypeOne + "/type/equals/bool"))
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
	var ds DataSource
	err = json.Unmarshal(b, &ds)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if ds.Type != "bool" {
		t.Errorf("Instead of the expected datasource (Type:bool), it returned:\n%+v", ds)
	}

	// Search for data sources that contains "sensor" in Resource
	res, err = http.Get(filterURL(FTypeMany + "/resource/contains/dimmer.eu/sensor"))
	if err != nil {
		t.Fatalf(err.Error())
	}
	b, err = ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatalf(err.Error())
	}
	var reg Registry
	err = json.Unmarshal(b, &reg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Check if the total is correct
	if reg.Total != 2 || len(reg.Entries) != 2 {
		t.Errorf("Catalog contains total %d(%d entries) instead of 2 data sources:\n %+v", reg.Total, len(reg.Entries), reg)
	}
	// Check if correct entries are queried
	for _, ds := range reg.Entries {
		if !strings.Contains(ds.Resource, "sensor") {
			t.Errorf("Catalog entry resource contains something other than 'sensor': %+v", ds.Resource)
		}
	}
}

// A pool of bad data sources
var (
	invalidBodies = []string{
		// Provided id //////////
		`{
			"id": "12345",
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Provided url //////////
		`{
			"url": "any_regurl",
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Provided data url //////////
		`{
			"data" : "any_dataurl",
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Invalid retention policy //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Invalid retention duration //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3s"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Missing format //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3w"
			},
			"type": "string",
			"format": ""
		}`,
		//		// Float type and missing aggregation //////////
		//		`{
		//			"resource": "any_url",
		//			"meta": {},
		//			"retention": {
		//			    "policy": "2h",
		//			    "duration": "3w"
		//			},
		//			"aggregation": [],
		//			"type": "float",
		//			"format": "any_format"
		//		}`,

	}

	invalidPostBodies = []string{
		// Missing resource url //////////
		`{
			"resource": "",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
		// Missing type //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "",
			"format": "any_format"
		}`,
		// Invalid type //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3w"
			},
			"type": "some_unsupported_type",
			"format": "any_format"
		}`,
	}

	invalidPutBodies = []string{
		// Provided read-only resource url //////////
		`{
			"resource": "any_url",
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"format": "any_format"
		}`,
		// Provided read-only type //////////
		`{
			"retention": {
			    "policy": "2h",
			    "duration": "3d"
			},
			"type": "string",
			"format": "any_format"
		}`,
	}
)
