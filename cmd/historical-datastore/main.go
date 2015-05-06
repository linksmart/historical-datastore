package main

import (
	"flag"
	"net/http"
	"net/url"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/context"
	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/justinas/alice"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)

func main() {
	var addr = flag.String("addr", ":8080", "HTTP bind address")

	flag.Parse()
	// TODO config file

	// Setup and run the notifier
	nt := common.SetupNotifier()

	// registry
	regStorage := registry.NewMemoryStorage()
	regAPI := registry.NewRegistryAPI(regStorage, nt.Sender())

	// data
	u, _ := url.Parse("http://localhost:8086")
	dataStorageCfg := data.InfluxStorageConfig{
		URL:      *u,
		Database: "test",
	}
	dataStorage, _ := data.NewInfluxStorage(&dataStorageCfg)
	registryClient := registry.NewLocalClient(regStorage)

	dataAPI := data.NewDataAPI(registryClient, dataStorage, nt.NewReader())

	commonHandlers := alice.New(
		context.ClearHandler,
		loggingHandler,
		recoverHandler,
	)

	// http api
	router := newRouter()

	// generic handlers
	router.get("/health", commonHandlers.ThenFunc(healthHandler))
	router.get("/", commonHandlers.ThenFunc(indexHandler))

	// registry api
	router.get("/registry", commonHandlers.ThenFunc(regAPI.Index))
	router.post("/registry/", commonHandlers.ThenFunc(regAPI.Create))
	router.get("/registry/{id}", commonHandlers.ThenFunc(regAPI.Retrieve))
	router.put("/registry/{id}", commonHandlers.ThenFunc(regAPI.Update))
	router.delete("/registry/{id}", commonHandlers.ThenFunc(regAPI.Delete))
	router.get("/registry/{path}/{type}/{op}/{value}", commonHandlers.ThenFunc(regAPI.Filter))

	// data api
	router.post("/data/{id}", commonHandlers.ThenFunc(dataAPI.Submit))
	router.get("/data/{id}", commonHandlers.ThenFunc(dataAPI.Query))

	// aggregation api

	// start http server
	http.ListenAndServe(*addr, router)
}
