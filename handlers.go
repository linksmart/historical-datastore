// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"
	"net/http"
	"time"

	"runtime/debug"

	"code.linksmart.eu/hds/historical-datastore/common"
	"github.com/codegangsta/negroni"
)

func loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		nw := negroni.NewResponseWriter(w)
		logger.Debugf("\"%s %s\"\n", r.Method, r.URL.String())
		next.ServeHTTP(nw, r)
		logger.Printf("\"%s %s %s\" %d %d %v\n", r.Method, r.URL.String(), r.Proto, nw.Status(), nw.Size(), time.Now().Sub(t1))
	}
	return http.HandlerFunc(fn)
}

func recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				logger.Printf("PANIC: %v\n%v", r, string(debug.Stack()))
				http.Error(w, http.StatusText(500), 500)
			}
		}()
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "Historical Datastore v%s - Welcome!\n", common.APIVersion)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "OK")
}

func optionsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
