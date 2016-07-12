// Copyright 2016 Fraunhofer Insitute for Applied Information Technology FIT

package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/codegangsta/negroni"
)

func loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		nw := negroni.NewResponseWriter(w)
		next.ServeHTTP(nw, r)
		log.Printf("[%s] %q %d %v\n", r.Method, r.URL.String(), nw.Status(), time.Now().Sub(t1))
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

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "Welcome!")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "OK")
}

func optionsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
