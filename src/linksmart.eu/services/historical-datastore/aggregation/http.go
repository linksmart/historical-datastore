package aggregation

import (
	"net/http"

	"linksmart.eu/services/historical-datastore/registry"
)

type API struct {
}

func NewAPI(registryClient registry.Client, aggr Aggr) *API {
	return &API{}
}

func (api *API) Query(w http.ResponseWriter, r *http.Request) {

}
