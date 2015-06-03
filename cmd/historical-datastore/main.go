package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/gorilla/context"
	"linksmart.eu/services/historical-datastore/Godeps/_workspace/src/github.com/justinas/alice"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"
)

var confPath = flag.String("conf", "historical-datastore.json", "Historical Datastore configuration file path")

func main() {
	flag.Parse()

	// Load Config File
	conf, err := loadConfig(confPath)
	if err != nil {
		fmt.Printf("Config File: %s\n", err)
		os.Exit(1)
	}

	// registry
	regStorage, ntSndRegCh := registry.NewMemoryStorage()
	regAPI := registry.NewWriteableAPI(regStorage)

	// data
	dataStorage, _, ntRcvDataCh := data.NewInfluxStorage(&conf.InfluxConf)
	registryClient := registry.NewLocalClient(regStorage)
	dataAPI := data.NewWriteableAPI(registryClient, dataStorage)

	// aggregation
	// TODO

	// Start the notifier
	common.StartNotifier(ntSndRegCh, ntRcvDataCh)

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
	router.get("/registry/{type}/{path}/{op}/{value:.*}", commonHandlers.ThenFunc(regAPI.Filter))

	// data api
	router.post("/data/{id}", commonHandlers.ThenFunc(dataAPI.Submit))
	router.get("/data/{id}", commonHandlers.ThenFunc(dataAPI.Query))

	// aggregation api
	// TODO

	// start http server
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.Http.BindAddr, conf.Http.BindPort), router)
	if err != nil {
		fmt.Println(err.Error())
	}
}
