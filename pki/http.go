package pki

import (
	"io/ioutil"
	"net/http"

	"github.com/linksmart/historical-datastore/common"
)

// API describes the RESTful HTTP data API
type API struct {
	ca *CertificateAuthority
}

func NewAPI(ca *CertificateAuthority) *API {
	a := &API{ca}
	return a
}

func (a *API) Sign(w http.ResponseWriter, r *http.Request) {
	if a.ca == nil { // If CA is not setup, this API will respond with error status.
		common.HttpErrorResponse(&common.BadRequestError{S: "Certification Authority is not set up"}, w)
		return
	}
	// Read body
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}

	csr, err := PEMToCSR(body)
	if err != nil {
		common.HttpErrorResponse(&common.BadRequestError{S: err.Error()}, w)
		return
	}
	cert, err := a.ca.CreateCertificate(csr, false)
	if err != nil {
		common.HttpErrorResponse(&common.InternalError{S: err.Error()}, w)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/x-pem-file")
	w.Write(cert)
}
