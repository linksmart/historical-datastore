package pki

// API describes the RESTful HTTP data API
type API struct {
	ca *CertificateAuthority
}

func newAPI(ca *CertificateAuthority) *API {
	a := &API{ca}
	return a
}

func (a *API) CreateCertificate() {

}
