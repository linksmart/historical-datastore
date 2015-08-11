package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	gocas "linksmart.eu/auth/cas"
	common "linksmart.eu/services/historical-datastore/common"
)

var serviceToken string
var cas *gocas.CasAuth
var serviceID = "testServiceID"

func obtainServiceToken() error {
	casConfPath := flag.String("cas-conf", "cas-auth.json", "CAS Authenticator configuration file path")

	// Setup CAS Authenticator with conf file
	var err error
	cas, err = gocas.SetupCasAuth(casConfPath)
	if err != nil {
		return err
	}

	// Get Ticket Granting Ticket
	TGT, err := cas.RequestTicketGrantingTicket()
	if err != nil {
		return err
	}

	// Get Service Token
	serviceToken, err = cas.RequestServiceToken(TGT, serviceID)
	if err != nil {
		return err
	}

	return nil
}

func authHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {

		// Validate Token
		valid, body, err := cas.ValidateServiceToken(serviceID, serviceToken)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		}
		if !valid {
			if _, ok := body["error"]; ok {
				log.Printf("[%s] %q %s\n", r.Method, r.URL.String(), body["error"])
				common.ErrorResponse(http.StatusUnauthorized, "Unauthorized request: "+body["error"].(string), w)
			}
		} else {
			next.ServeHTTP(w, r)
		}
	}
	return http.HandlerFunc(fn)
}

func loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[%s] %q %v\n", r.Method, r.URL.String(), time.Now().Sub(t1))
	}
	return http.HandlerFunc(fn)
}

func recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("PANIC: %+v", err)
				http.Error(w, http.StatusText(500), 500)
			}
		}()
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
