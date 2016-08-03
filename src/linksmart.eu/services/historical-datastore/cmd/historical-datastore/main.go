// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/gorilla/context"
	"github.com/justinas/alice"

	"linksmart.eu/services/historical-datastore/aggregation"
	"linksmart.eu/services/historical-datastore/common"
	"linksmart.eu/services/historical-datastore/data"
	"linksmart.eu/services/historical-datastore/registry"

	_ "linksmart.eu/lc/sec/auth/cas/validator"
	"linksmart.eu/lc/sec/auth/validator"
)

var (
	confPath = flag.String("conf", "conf/historical-datastore.json", "Historical Datastore configuration file path")
)

func main() {
	flag.Parse()

	// Load Config File
	conf, err := loadConfig(confPath)
	if err != nil {
		logger.Fatalf("Config File: %s\n", err)
	}

	// registry
	var (
		regStorage registry.Storage
		ntSndRegCh *chan common.Notification
		closeReg   func() error
	)
	switch conf.Reg.Backend.Type {
	case "memory":
		regStorage, ntSndRegCh = registry.NewMemoryStorage()
	case "leveldb":
		regStorage, ntSndRegCh, closeReg, err = registry.NewLevelDBStorage(conf.Reg.Backend.DSN, nil)
		if err != nil {
			logger.Fatalf("Failed to start LevelDB: %s\n", err)
		}
	}

	regAPI := registry.NewWriteableAPI(regStorage)
	registryClient := registry.NewLocalClient(regStorage)

	// data
	influxStorage, ntRcvDataCh, _ := data.NewInfluxStorage(conf.Data.Backend.DSN)
	dataAPI := data.NewWriteableAPI(registryClient, influxStorage)

	// aggregation
	dataAggr, ntRcvAggrCh, _ := aggregation.NewInfluxAggr(influxStorage)
	aggrAPI := aggregation.NewAPI(registryClient, dataAggr)

	// Start the notifier
	common.StartNotifier(ntSndRegCh, ntRcvDataCh, ntRcvAggrCh)

	commonHandlers := alice.New(
		context.ClearHandler,
		loggingHandler,
		recoverHandler,
		commonHeaders,
	)

	// http api
	router := newRouter()
	router.options("/{path:.*}", commonHandlers.ThenFunc(optionsHandler))

	// Append auth handler if enabled
	if conf.Auth.Enabled {
		// Setup ticket validator
		v, err := validator.Setup(conf.Auth.Provider, conf.Auth.ProviderURL, conf.Auth.ServiceID, conf.Auth.Authz)
		if err != nil {
			logger.Fatalf(err.Error())
		}

		commonHandlers = commonHandlers.Append(v.Handler)
	}

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
	router.get("/aggr", commonHandlers.ThenFunc(aggrAPI.Index))
	router.get("/aggr/{path}/{op}/{value:.*}", commonHandlers.ThenFunc(aggrAPI.Filter))
	router.get("/aggr/{aggrid}/{uuid}", commonHandlers.ThenFunc(aggrAPI.Query))

	// Register in the service catalog(s)
	var wg sync.WaitGroup
	regChannels := registerInServiceCatalog(conf, &wg)

	// Ctrl+C / Kill handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)
	go func() {
		<-handler
		logger.Println("Shutting down...")

		// Unregister in the service catalog(s)
		for _, sigCh := range regChannels {
			// Notify if the routine hasn't returned already
			select {
			case sigCh <- true:
			default:
			}
		}
		wg.Wait()

		// Close the Registry Storage
		if closeReg != nil {
			err := closeReg()
			if err != nil {
				logger.Println(err.Error())
			}
		}

		logger.Println("Stopped.")
		os.Exit(0)
	}()

	// Serve static web directory
	go webServer(conf)

	// start http server
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort), router)
	if err != nil {
		logger.Fatalln(err)
	}
}

func webServer(conf *Config) {
	staticConf := map[string]interface{}{
		"hdsEndpoint": conf.HTTP.PublicEndpoint,
	}

	if conf.Auth.Enabled {
		staticConf["authEnabled"] = conf.Auth.Enabled
		staticConf["authProvider"] = conf.Auth.Provider
		staticConf["authProviderURL"] = conf.Auth.ProviderURL
		staticConf["authServiceID"] = conf.Auth.ServiceID
	}

	b, err := json.Marshal(staticConf)
	if err != nil {
		logger.Fatalln("Error marshalling web config file:", err)
	}

	err = os.MkdirAll(conf.Web.StaticDir+"/conf", 0755)
	if err != nil {
		logger.Fatalln("Error writing web config file:", err)
	}

	err = ioutil.WriteFile(conf.Web.StaticDir+"/conf/autogen_config.json", b, 0644)
	if err != nil {
		logger.Fatalln("Error writing web config file:", err)
	}

	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir(conf.Web.StaticDir))
	mux.Handle("/", fs)
	go func() {
		err := http.ListenAndServe(fmt.Sprintf("%s:%d", conf.Web.BindAddr, conf.Web.BindPort), mux)
		if err != nil {
			logger.Fatalln(err)
		}
	}()
}
