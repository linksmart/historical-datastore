package registry

import (
	"fmt"
	"net/http"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/mux"
)

// Index is a handler for the registry index
func Index(w http.ResponseWriter, r *http.Request) {
	// TODO
	fmt.Fprintf(w, "TODO registry index")
}

// Create is a handler for creating a new DataSource
func Create(w http.ResponseWriter, r *http.Request) {
	// TODO
	fmt.Fprintf(w, "TODO registry create")
}

// Retrieve is a handler for retrieving a new DataSource
// Expected parameters: id
func Retrieve(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	// TODO
	fmt.Fprintf(w, "TODO registry retrieve %v", id)
}

// Update is a handler for updating the given DataSource
// Expected parameters: id
func Update(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	// TODO
	fmt.Fprintf(w, "TODO registry update %v", id)
}

// Delete is a handler for deleting the given DataSource
// Expected parameters: id
func Delete(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id := params["id"]
	// TODO
	fmt.Fprintf(w, "TODO registry delete %v", id)
}

// Filter is a handler for registry filtering API
// Expected parameters: path, type, op, value
func Filter(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	fpath := params["path"]
	ftype := params["type"]
	fop := params["op"]
	fvalue := params["value"]
	// TODO
	fmt.Fprintf(w, "TODO registry filter %v/%v/%v/%v", fpath, ftype, fop, fvalue)
}
