package main

import (
	"flag"
	"fmt"
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
		fmt.Printf("Config File: %s\n", err)
		os.Exit(1)
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
			fmt.Printf("Failed to start LevelDB: %s\n", err)
			os.Exit(1)
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
	)

	// Append auth handler if enabled
	if conf.Auth.Enabled {
		// Setup ticket validator
		v, err := validator.Setup(conf.Auth.Provider, conf.Auth.ProviderURL, conf.Auth.ServiceID, conf.Auth.Authz)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		commonHandlers = commonHandlers.Append(v.Handler)
	}

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
		fmt.Println(" Shutting down...")

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
				fmt.Println(err.Error())
			}
		}

		fmt.Println("Stopped.")
		os.Exit(0)
	}()

	// start http server
	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort), router)
	if err != nil {
		fmt.Println(err.Error())
	}
}
