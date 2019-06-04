// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/codegangsta/negroni"
	"github.com/gorilla/context"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/linksmart/historical-datastore/common"
	"github.com/rs/cors"
)

type router struct {
	*mux.Router
	alice.Chain
}

func newRouter() *router {
	r := router{Router: mux.NewRouter().
		StrictSlash(false).
		SkipClean(true), //Enable complex urls in senml name. like "/data/http://example.com/temperatureSensor"
	}

	// default handler(s)
	r.handle(http.MethodGet, "/health", healthHandler)

	// middleware chain for handler, used when calling chained()
	r.Chain = alice.New(
		context.ClearHandler,
		loggingHandler,
		recoverHandler,
		cors.AllowAll().Handler,
	)

	return &r
}

func (r *router) appendChain(handler alice.Constructor) {
	r.Chain = r.Chain.Append(handler)
}

// chained chains the middleware and returns the final handler
func (r *router) chained() http.Handler {
	return r.Then(r)
}

func (r *router) handle(m, p string, f http.HandlerFunc) {
	// handle with and without slash (no redirects)
	r.Methods(m).Path(p).HandlerFunc(f)
	r.Methods(m).Path(fmt.Sprintf("%s/", p)).HandlerFunc(f)
}

func loggingHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		t1 := time.Now()
		nw := negroni.NewResponseWriter(w)
		// log.Printf("\"%s %s\"\n", r.Method, r.BrokerURL.String())
		next.ServeHTTP(nw, r)
		log.Printf("\"%s %s %s\" %d %d %v\n", r.Method, r.URL.String(), r.Proto, nw.Status(), nw.Size(), time.Now().Sub(t1))
	}
	return http.HandlerFunc(fn)
}

func recoverHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("PANIC: %v\n%v", r, string(debug.Stack()))
				http.Error(w, http.StatusText(500), 500)
			}
		}()
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", common.DefaultMIMEType)
	fmt.Fprintf(w, "{\"status\":\"OK\"}")
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Historical Datastore %s - Welcome!\n", common.APIVersion)
}
