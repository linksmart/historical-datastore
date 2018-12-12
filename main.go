// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"

	_ "code.linksmart.eu/com/go-sec/auth/keycloak/validator"
	"code.linksmart.eu/com/go-sec/auth/validator"
	"code.linksmart.eu/hds/historical-datastore/aggregation"
	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/gorilla/context"
	"github.com/justinas/alice"
)

const LINKSMART = `
╦   ╦ ╔╗╔ ╦╔═  ╔═╗ ╔╦╗ ╔═╗ ╦═╗ ╔╦╗ R
║   ║ ║║║ ╠╩╗  ╚═╗ ║║║ ╠═╣ ╠╦╝  ║
╩═╝ ╩ ╝╚╝ ╩ ╩  ╚═╝ ╩ ╩ ╩ ╩ ╩╚═  ╩
`

var (
	confPath    = flag.String("conf", "conf/historical-datastore.json", "Historical Datastore configuration file path")
	profile     = flag.Bool("profile", false, "Enable the HTTP server for runtime profiling")
	version     = flag.Bool("version", false, "Show the Historical Datastore API version")
	Version     = "N/A" // set with build flags
	BuildNumber = "N/A" // set with build flags
)

func main() {
	flag.Parse()
	if *version {
		fmt.Println(Version)
		return
	}
	fmt.Print(LINKSMART)
	log.Printf("Starting Historical Datastore")
	log.Printf("Version: %s", Version)
	log.Printf("Build Number: %s", BuildNumber)
	common.APIVersion = Version

	if *profile {
		log.Println("Starting runtime profiling server")
		go func() { log.Println(http.ListenAndServe("0.0.0.0:6060", nil)) }()
	}

	// Load Config File
	conf, err := loadConfig(confPath)
	if err != nil {
		log.Fatalf("Config File: %s\n", err)
	}

	// Setup data and aggregation backends
	var (
		dataStorage data.Storage
		aggrStorage aggregation.Storage
	)
	switch conf.Data.Backend.Type {
	case data.INFLUXDB:
		dataStorage, err = data.NewInfluxStorage(conf.Data, conf.Reg.RetentionPeriods)
		if err != nil {
			log.Fatalf("Error creating influx storage: %v", err)
		}
		aggrStorage, err = aggregation.NewInfluxAggr(dataStorage.(*data.InfluxStorage))
		if err != nil {
			log.Fatalf("Error creating influx aggr: %v", err)
		}
	}
	if conf.Data.AutoRegistration {
		log.Println("Auto Registration is enabled: Data HTTP API will automatically create new data sources.")
	}
	// MQTT connector
	mqttConn, err := data.NewMQTTConnector(dataStorage)
	if err != nil {
		log.Fatalf("Error creating MQTT Connector: %v", err)
	}

	// Setup registry
	var (
		regStorage registry.Storage
		closeReg   func() error
	)
	switch conf.Reg.Backend.Type {
	case registry.MEMORY:
		regStorage = registry.NewMemoryStorage(conf.Reg, dataStorage, aggrStorage, mqttConn)
	case registry.LEVELDB:
		regStorage, closeReg, err = registry.NewLevelDBStorage(conf.Reg, nil, dataStorage, aggrStorage, mqttConn)
		if err != nil {
			log.Fatalf("Failed to start LevelDB: %s\n", err)
		}
	}

	// Setup APIs
	regAPI := registry.NewAPI(regStorage)
	dataAPI := data.NewAPI(regStorage, dataStorage, conf.Data.AutoRegistration)
	aggrAPI := aggregation.NewAPI(regStorage, aggrStorage)

	// Start MQTT connector
	// TODO: disconnect on shutdown
	err = mqttConn.Start(regStorage)
	if err != nil {
		log.Fatalf("Error starting MQTT Connector: %v", err)
	}

	// Register in the service catalog(s)
	unregisterService := registerInServiceCatalog(conf)

	// Start servers
	go startHTTPServer(conf, regAPI, dataAPI, aggrAPI)
	go startWebServer(conf)

	// Ctrl+C / Kill handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)

	<-handler
	log.Println("Shutting down...")

	// Unregister from the service catalog(s)
	unregisterService()

	// Close the Registry Storage
	if closeReg != nil {
		err := closeReg()
		if err != nil {
			log.Println(err.Error())
		}
	}

	log.Println("Stopped.")
}

func startHTTPServer(conf *common.Config, reg *registry.API, data *data.API, aggr *aggregation.API) {
	commonHandlers := alice.New(
		context.ClearHandler,
		loggingHandler,
		recoverHandler,
		commonHeaders,
	)

	// http api
	router := newRouter()
	// generic handlers
	router.get("/health", commonHandlers.ThenFunc(healthHandler))
	router.options("/{path:.*}", commonHandlers.ThenFunc(optionsHandler))

	// Append auth handler if enabled
	if conf.Auth.Enabled {
		// Setup ticket validator
		v, err := validator.Setup(
			conf.Auth.Provider,
			conf.Auth.ProviderURL,
			conf.Auth.ServiceID,
			conf.Auth.BasicEnabled,
			conf.Auth.Authz)
		if err != nil {
			log.Fatalf(err.Error())
		}

		commonHandlers = commonHandlers.Append(v.Handler)
	}

	// api root
	router.get("/", commonHandlers.ThenFunc(indexHandler))

	// registry api
	router.get("/registry", commonHandlers.ThenFunc(reg.Index))
	router.post("/registry", commonHandlers.ThenFunc(reg.Create))
	router.get("/registry/{id}", commonHandlers.ThenFunc(reg.Retrieve))
	router.put("/registry/{id}", commonHandlers.ThenFunc(reg.Update))
	router.delete("/registry/{id}", commonHandlers.ThenFunc(reg.Delete))
	router.get("/registry/{type}/{path}/{op}/{value:.*}", commonHandlers.ThenFunc(reg.Filter))

	// data api
	router.post("/data", commonHandlers.ThenFunc(data.SubmitWithoutID))
	router.post("/data/{id}", commonHandlers.ThenFunc(data.Submit))
	router.get("/data/{id}", commonHandlers.ThenFunc(data.Query))

	// aggregation api
	router.get("/aggr", commonHandlers.ThenFunc(aggr.Index))
	router.get("/aggr/{path}/{op}/{value:.*}", commonHandlers.ThenFunc(aggr.Filter))
	router.get("/aggr/{aggrid}/{uuid}", commonHandlers.ThenFunc(aggr.Query))

	// start http server
	log.Printf("Listening on %s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort)
	err := http.ListenAndServe(fmt.Sprintf("%s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort), router)
	if err != nil {
		log.Fatalln(err)
	}
}

func startWebServer(conf *common.Config) {
	staticConf := map[string]interface{}{
		"apiPort": conf.HTTP.BindPort,
	}

	if conf.Auth.Enabled {
		staticConf["authEnabled"] = conf.Auth.Enabled
		staticConf["authProvider"] = conf.Auth.Provider
		staticConf["authProviderURL"] = conf.Auth.ProviderURL
		staticConf["authServiceID"] = conf.Auth.ServiceID
	}

	b, err := json.Marshal(staticConf)
	if err != nil {
		log.Fatalln("Error marshalling web config file:", err)
	}

	err = os.MkdirAll(conf.Web.StaticDir+"/conf", 0755)
	if err != nil {
		log.Fatalln("Error writing web config file:", err)
	}

	err = ioutil.WriteFile(conf.Web.StaticDir+"/conf/autogen_config.json", b, 0644)
	if err != nil {
		log.Fatalln("Error writing web config file:", err)
	}

	mux := http.NewServeMux()
	fs := http.FileServer(http.Dir(conf.Web.StaticDir))
	mux.Handle("/", fs)

	err = http.ListenAndServe(fmt.Sprintf("%s:%d", conf.Web.BindAddr, conf.Web.BindPort), mux)
	if err != nil {
		log.Fatalln(err)
	}
}
