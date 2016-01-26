package aggregation

import "net/http"

type API struct {
}

func NewAPI() *API {
	return &API{}
}

func (api *API) Query(w http.ResponseWriter, r *http.Request) {

}
