// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"time"

	_ "github.com/linksmart/go-sec/auth/keycloak/validator"
	"github.com/linksmart/go-sec/auth/validator"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/demo"
	"github.com/linksmart/historical-datastore/registry"
	"github.com/oleksandr/bonjour"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const LINKSMART = `
╦   ╦ ╔╗╔ ╦╔═  ╔═╗ ╔╦╗ ╔═╗ ╦═╗ ╔╦╗
║   ║ ║║║ ╠╩╗  ╚═╗ ║║║ ╠═╣ ╠╦╝  ║
╩═╝ ╩ ╝╚╝ ╩ ╩  ╚═╝ ╩ ╩ ╩ ╩ ╩╚═  ╩
`

var (
	Version     string // set with build flags
	BuildNumber string // set with build flags
)

func main() {
	var (
		confPath = flag.String("conf", "conf/historical-datastore.json", "Historical Datastore configuration file path")
		profile  = flag.Bool("profile", false, "Enable the HTTP server for runtime profiling")
		version  = flag.Bool("version", false, "Show the Historical Datastore API version")
		demomode = flag.Bool("demo", false, "Run Historical Datastore in demo mode. This creates the service with a growing dummy data.\n"+
			"By default the data will not be persistent. Inorder to run hds in persistent mode use \"-persistent\" flag")
		persistentDemo = flag.Bool("persistent", false, "While running Historical Datastore in demo mode, use persistent storage location specified"+
			" in the config file")
		ignoreEnv = flag.Bool("ignore-env", false, "Do not override the configurations by environmental variables. If this flag is enabled, only configuration file is considered")
	)
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
	conf, err := loadConfig(confPath, *ignoreEnv)
	if err != nil {
		log.Panicf("Config File: %s\n", err)
	}

	fmt.Println(os.Getenv("HDS_REGISTRY_BACKEND_DSN"))
	if common.Debug {
		log.Println("===========================")
		log.Println(conf.String())
		log.Println("===========================")
	}
	if *demomode {

		log.Println("===========================")
		log.Printf("RUNNING IN DEMO MODE")
		log.Println("===========================")

		if !*persistentDemo {
			conf.Data.Backend.DSN = os.TempDir() + string(os.PathSeparator) + "hds_demo_" + strconv.FormatInt(time.Now().UnixNano(), 10)
			//use memory in demo mode for registry
			conf.Registry.Backend.Type = registry.MEMORY
			defer os.Remove(conf.Data.Backend.DSN) //remove the temporary file if created on exit
		} else {
			log.Printf("Storing registry data in %s.", conf.Registry.Backend.DSN)
		}
		log.Printf("Storing senml data in %s.", conf.Data.Backend.DSN)

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

	switch conf.Data.Backend.Type {
	case data.SQLITE:
		var disconnect_func func() error
		dataStorage, disconnect_func, err = data.NewSqlStorage(conf.Data)
		if err != nil {
			log.Panicf("Error creating SQLite storage: %s", err)
		}
		defer disconnect_func()
	}
	if conf.Data.AutoRegistration {
		log.Println("Auto Registration is enabled: Data HTTP API will automatically create new time series.")
	}

	// Setup registry
	var (
		regStorage registry.Storage
		closeReg   func() error
		mqttConn   *data.MQTTConnector
	)

	// MQTT connector
	mqttConn, err = data.NewMQTTConnector(dataStorage, conf.ServiceID)
	if err != nil {
		log.Panicf("Error creating MQTT Connector: %s", err)
	}

	switch conf.Registry.Backend.Type {
	case registry.MEMORY:
		regStorage = registry.NewMemoryStorage(conf.Registry, dataStorage, mqttConn)
	case registry.LEVELDB:
		regStorage, closeReg, err = registry.NewLevelDBStorage(conf.Registry, nil, dataStorage, mqttConn)
		if err != nil {
			log.Panicf("Failed to start LevelDB: %s\n", err)
		}
	}

	// Setup APIs
	regController := registry.NewController(regStorage)
	dataController := data.NewController(*regController, dataStorage, conf.Data.AutoRegistration)
	regAPI := registry.NewAPI(*regController)
	dataAPI := data.NewAPI(*dataController)
	//aggrAPI := aggregation.NewAPI(regStorage, aggrStorage)

	if *demomode {
		err = demo.StartDummyStreamer(*regController, *dataController)
		if err != nil {
			log.Panic("Failed to start the dummy streamer", err)
		}
	}
	// Start MQTT connector
	// TODO: disconnect on shutdown
	err = mqttConn.Start(*regController)
	if err != nil {
		log.Panicf("Error starting MQTT Connector: %s", err)
	}

	// Register in the LinkSmart Service Catalog
	if conf.ServiceCatalog.Enabled {
		unregisterService, err := registerInServiceCatalog(conf)
		if err != nil {
			log.Panicf("Error registering service: %s", err)
		}
		// Unregister from the Service Catalog
		defer unregisterService()
	}

	// Start servers
	go startHTTPServer(conf, regAPI, dataAPI)

	if conf.GRPC.Enabled {
		err = checkServerCertificate(conf.PKI)
		if err != nil {
			log.Printf("In order to run GRPC server, valid Server certificate key file, Server Cert file and CA Cert file must be set in conf.pki setting")
			log.Panicf("Error setting up server certificates: %s", err)
		}
		go startGRPCServer(conf, dataController, regController)
	}
	// Announce service using DNS-SD
	var bonjourS *bonjour.Server
	if conf.DnssdEnabled {
		go func() {
			bonjourS, err = bonjour.Register(conf.Description,
				common.DNSSDServiceType,
				"",
				int(conf.HTTP.BindPort),
				[]string{"uri=/"},
				nil)
			if err != nil {
				log.Printf("Failed to register DNS-SD service: %s", err.Error())
				return
			}
			log.Println("Registered service via DNS-SD using type", common.DNSSDServiceType)
		}()
	}

	// Ctrl+C / Kill handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)

	<-handler
	log.Println("Shutting down...")

	// Stop bonjour registration
	if bonjourS != nil {
		bonjourS.Shutdown()
		time.Sleep(1e9)
	}
	// Close the registry Storage
	if closeReg != nil {
		err := closeReg()
		if err != nil {
			log.Println(err.Error())
		}
	}

	log.Println("Stopped.")
}

func checkServerCertificate(pkiConf common.PKI) error {
	if pkiConf.ServerKey == "" || pkiConf.ServerCert == "" || pkiConf.CaCert == "" {
		return fmt.Errorf("server certificate, key and ca files are not set")
	}
	if !fileExists(pkiConf.ServerKey) || !fileExists(pkiConf.ServerCert) || !fileExists(pkiConf.CaCert) {
		return fmt.Errorf("server certificate, key and ca files are not valid")
	}
	return nil
}

func startGRPCServer(conf *common.Config, dataController *data.Controller, regController *registry.Controller) {
	serverAddr := fmt.Sprintf("%s:%d", conf.GRPC.BindAddr, conf.GRPC.BindPort)

	log.Printf("Serving GRPC on %s", serverAddr)
	serverCertFile := conf.PKI.ServerCert
	serverPrivatekey := conf.PKI.ServerKey
	caFile := conf.PKI.CaCert
	// Load the certificates from disk
	certificate, err := tls.LoadX509KeyPair(serverCertFile, serverPrivatekey)
	if err != nil {
		log.Panicf("could not load server key pair: %s", err)
	}

	// Create a certificate pool from the certificate authority
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(caFile)
	if err != nil {
		log.Panicf("could not read ca certificate: %s", err)
	}

	// Append the client certificates from the CA
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Fatalf("failed to append client certs")
	}
	l, err := net.Listen("tcp", serverAddr)
	if err != nil {
		log.Fatalf("could not listen to %s: %v", serverAddr, err)
	}
	// Create the TLS credentials
	creds := credentials.NewTLS(&tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    certPool,
	})

	srv := grpc.NewServer(grpc.Creds(creds))

	data.RegisterGRPCAPI(srv, *dataController)
	registry.RegisterGRPCAPI(srv, *regController)

	err = srv.Serve(l)

	if err != nil {
		log.Fatalf("Stopped listening GRPC: %v", err)
	}
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
	router.handle(http.MethodPut, "/registry/{id:.+}", reg.UpdateOrCreate)
	router.handle(http.MethodDelete, "/registry/{id:.+}", reg.Delete)

	// data api
	router.handle(http.MethodPost, "/data", data.SubmitWithoutID)
	router.handle(http.MethodPost, "/data/{id:.+}", data.Submit)
	router.handle(http.MethodGet, "/data/{id:.+}", data.Query)
	router.handle(http.MethodDelete, "/data/{id:.+}", data.Delete)

	// Append auth handler if enabled
	if conf.Auth.Enabled {
		// Setup ticket validator
		v, err := validator.Setup(conf.Auth.Provider, conf.Auth.ProviderURL, conf.Auth.ClientID, conf.Auth.BasicEnabled, &conf.Auth.Authz)
		if err != nil {
			log.Fatalf(err.Error())
		}

		router.appendChain(v.Handler)
	}
	// start http server
	serverUrl := fmt.Sprintf("%s:%d", conf.HTTP.BindAddr, conf.HTTP.BindPort)
	log.Printf("Serving HTTP requests on %s", serverUrl)
	err := http.ListenAndServe(serverUrl, router.chained())
	if err != nil {
		log.Fatalln(err)
	}

}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
