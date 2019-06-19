// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	_ "code.linksmart.eu/com/go-sec/auth/keycloak/validator"
	"code.linksmart.eu/com/go-sec/auth/validator"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/demo"
	"github.com/linksmart/historical-datastore/registry"
	uuid "github.com/satori/go.uuid"
)

const LINKSMART = `
╦   ╦ ╔╗╔ ╦╔═  ╔═╗ ╔╦╗ ╔═╗ ╦═╗ ╔╦╗
║   ║ ║║║ ╠╩╗  ╚═╗ ║║║ ╠═╣ ╠╦╝  ║
╩═╝ ╩ ╝╚╝ ╩ ╩  ╚═╝ ╩ ╩ ╩ ╩ ╩╚═  ╩
`

var (
	confPath    = flag.String("conf", "conf/historical-datastore.json", "Historical Datastore configuration file path")
	profile     = flag.Bool("profile", false, "Enable the HTTP server for runtime profiling")
	version     = flag.Bool("version", false, "Show the Historical Datastore API version")
	demomode    = flag.Bool("demo", false, "Run Historical Datasource in demo mode. This creates the service with a growing dummy data")
	Version     string // set with build flags
	BuildNumber string // set with build flags
)

func main() {
	flag.Parse()
	if *version {
		fmt.Println(Version)
		return
	}
	fmt.Print(LINKSMART)
	log.Printf("Starting Historical Datastore")
	if Version != "" {
		log.Printf("Version: %s", Version)
	}
	if BuildNumber != "" {
		log.Printf("Build Number: %s", BuildNumber)
	}

	common.SetVersion(Version)

	if *profile {
		log.Println("Starting runtime profiling server")
		go func() { log.Println(http.ListenAndServe("0.0.0.0:6060", nil)) }()
	}

	// Load Config File
	conf, err := loadConfig(confPath)
	if err != nil {
		log.Fatalf("Config File: %s\n", err)
	}
	if conf.ServiceID == "" {
		conf.ServiceID = uuid.NewV4().String()
		log.Printf("Service ID not set. Generated new UUID: %s", conf.ServiceID)
	}

	// Setup data and aggregation backends
	var (
		dataStorage data.Storage
		//aggrStorage aggregation.Storage
	)
	if *demomode {
		tempdsn := os.TempDir() + "/hds_demo_" + string(time.Now().UnixNano())
		backend := common.DataBackendConf{Type: "senmlstore", DSN: tempdsn}
		var disconnect_func func() error
		dataStorage, disconnect_func, err = data.NewSenmlStorage(common.DataConf{Backend: backend})
		if err != nil {
			log.Fatalf("Error creating senml storage: %s", err)
		}
		defer disconnect_func()
	} else {
		switch conf.Data.Backend.Type {
		case data.SENMLSTORE:
			var disconnect_func func() error
			dataStorage, disconnect_func, err = data.NewSenmlStorage(conf.Data)
			if err != nil {
				log.Fatalf("Error creating senml storage: %s", err)
			}
			defer disconnect_func()
		}
		if conf.Data.AutoRegistration {
			log.Println("Auto Registration is enabled: Data HTTP API will automatically create new data sources.")
		}
	}

	// Setup registry
	var (
		regStorage registry.Storage
		closeReg   func() error
		mqttConn   *data.MQTTConnector
	)

	if *demomode {
		//use memory in demo mode for registry
		conf.Reg.Backend.Type = registry.MEMORY
	} else {
		// MQTT connector
		mqttConn, err = data.NewMQTTConnector(dataStorage, conf.ServiceID)
		if err != nil {
			log.Fatalf("Error creating MQTT Connector: %s", err)
		}
	}

	switch conf.Reg.Backend.Type {
	case registry.MEMORY:
		regStorage = registry.NewMemoryStorage(conf.Reg, dataStorage, mqttConn)
	case registry.LEVELDB:
		regStorage, closeReg, err = registry.NewLevelDBStorage(conf.Reg, nil, dataStorage, mqttConn)
		if err != nil {
			log.Fatalf("Failed to start LevelDB: %s\n", err)
		}
	}

	// Setup APIs
	regAPI := registry.NewAPI(regStorage)
	dataAPI := data.NewAPI(regStorage, dataStorage, conf.Data.AutoRegistration)
	//aggrAPI := aggregation.NewAPI(regStorage, aggrStorage)

	if *demomode {
		go demo.DummyStreamer(regStorage, dataStorage)
	} else {
		// Start MQTT connector
		// TODO: disconnect on shutdown
		err = mqttConn.Start(regStorage)
		if err != nil {
			log.Fatalf("Error starting MQTT Connector: %s", err)
		}

		// Register in the LinkSmart Service Catalog
		if conf.ServiceCatalog != nil {
			unregisterService, err := registerInServiceCatalog(conf)
			if err != nil {
				log.Fatalf("Error registering service: %s", err)
			}
			// Unregister from the Service Catalog
			defer unregisterService()
		}

	}
	// Start servers
	go startHTTPServer(conf, regAPI, dataAPI)

	// Ctrl+C / Kill handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)

	<-handler
	log.Println("Shutting down...")

	// Close the DataStreamList Storage
	if closeReg != nil {
		err := closeReg()
		if err != nil {
			log.Println(err.Error())
		}
	}

	log.Println("Stopped.")
}

func startHTTPServer(conf *common.Config, reg *registry.API, data *data.API) {
	router := newRouter()
	// api root
	router.handle(http.MethodGet, "/", indexHandler)
	// registry api
	router.handle(http.MethodGet, "/registry", reg.Index)
	router.handle(http.MethodPost, "/registry", reg.Create)
	router.handle(http.MethodGet, "/registry/{type}/{path}/{op}/{value:.*}", reg.Filter) //TODO: Re-ordered this to match filtering.
	//Filter should go for separate endpoint?
	router.handle(http.MethodGet, "/registry/{id:.+}", reg.Retrieve)
	router.handle(http.MethodPut, "/registry/{id:.+}", reg.Update)
	router.handle(http.MethodDelete, "/registry/{id:.+}", reg.Delete)

	// data api
	router.handle(http.MethodPost, "/data", data.SubmitWithoutID)
	router.handle(http.MethodPost, "/data/{id:.+}", data.Submit)
	router.handle(http.MethodGet, "/data/{id:.+}", data.Query)
	// Append auth handler if enabled
	if conf.Auth.Enabled {
		// Setup ticket validator
		v, err := validator.Setup(conf.Auth.Provider, conf.Auth.ProviderURL, conf.Auth.ServiceID, conf.Auth.BasicEnabled, conf.Auth.Authz)
		if err != nil {
			log.Fatalf(err.Error())
		}

		router.appendChain(v.Handler)
	}
	// start http server
	serverUrl := fmt.Sprintf("%s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort)
	log.Printf("Listening on %s", serverUrl)
	err := http.ListenAndServe(serverUrl, router.chained())
	if err != nil {
		log.Fatalln(err)
	}

}
