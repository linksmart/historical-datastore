package registry

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
	"linksmart.eu/services/historical-datastore/common"
)

// Registry api
type RegistryAPI struct {
	storage         MemoryStorage
	host            string
	apiLocation     string
	dataAPILocation string
}

func NewRegistryAPI(host, apiLocation, dataAPILocation string) *RegistryAPI {
	return &RegistryAPI{host: host, apiLocation: apiLocation, dataAPILocation: dataAPILocation}
}

// Handlers ///////////////////////////////////////////////////////////////////////

// Index is a handler for the registry index
func (regAPI *RegistryAPI) Index(w http.ResponseWriter, r *http.Request) {
	// TODO
	fmt.Fprintf(w, "TODO registry index")
}

// Create is a handler for creating a new DataSource
func (regAPI *RegistryAPI) Create(w http.ResponseWriter, r *http.Request) {

	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()

	var ds DataSource
	err = unmarshalDataSource(body, &ds)
	if err != nil {
		common.ErrorResponse(http.StatusBadRequest, fmt.Sprint("Error processing input: ", err.Error()), w)
		return
	}
	ds.URL = regAPI.apiLocation      // append id after generation
	ds.Data = regAPI.dataAPILocation // append id after generation

	err = regAPI.storage.add(&ds)
	if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, fmt.Sprint("Error storing the datasource: ", err.Error()), w)
		return
	}

	fmt.Printf("%+v\n", ds)
	fmt.Println(regAPI.host)

	//w.Header().Set("Content-Type", "application/json;version="+common.APIVersion)
	w.Header().Set("Location", fmt.Sprintf("%s/%s", regAPI.host, ds.URL))
	w.WriteHeader(http.StatusCreated)
	return
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]

	w.Header().Set("Content-Type", "application/senml+json;version="+common.APIVersion)
	//w.Header().Set("Location", fmt.Sprintf("%s/%s", self.apiLocation, s.Id))
	w.WriteHeader(http.StatusCreated)

	ds, err := regAPI.storage.get(id)
	if err == ErrorNotFound {
		common.ErrorResponse(http.StatusNotFound, fmt.Sprint("DataSource not found: ", err.Error()), w)
		return
	} else if err != nil {
		common.ErrorResponse(http.StatusInternalServerError, fmt.Sprint("Error requesting registry: ", err.Error()), w)
		return
	}

	b, _ := json.Marshal(ds)

	w.Header().Set("Content-Type", "application/senml+json;version="+common.APIVersion)
	w.Write(b)

	return
}

// Update is a handler for updating the given DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Update(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	// TODO
	fmt.Fprintf(w, "TODO registry update %v", id)
}

// Delete is a handler for deleting the given DataSource
// Expected parameters: id
func (regAPI *RegistryAPI) Delete(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	// TODO
	fmt.Fprintf(w, "TODO registry delete %v", id)
}

// Filter is a handler for registry filtering API
// Expected parameters: path, type, op, value
func (regAPI *RegistryAPI) Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	ftype := params["type"]
	fop := params["op"]
	fvalue := params["value"]
	// TODO
	fmt.Fprintf(w, "TODO registry filter %v/%v/%v/%v", fpath, ftype, fop, fvalue)
}

///////////////////////////////////////////////////////////////////////////////////

// Unmarshalls json and parses the string of resource url
func unmarshalDataSource(body []byte, ds *DataSource) error {
	// Unmarshal body
	err := json.Unmarshal(body, ds)
	if err != nil {
		return err
	}

	// Unmarshal the resource string seperately
	type RawDataSource struct {
		Resource string `json:"resource"`
	}
	var rds RawDataSource
	err = json.Unmarshal(body, &rds)
	if err != nil {
		return err
	}

	// Parse it into URL
	resourceURL, err := url.Parse(rds.Resource)
	if err != nil {
		return err
	}

	// Add it to DataSource
	ds.Resource = *resourceURL

	return nil
}
