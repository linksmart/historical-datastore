// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
)

var (
	config = flag.String("conf", "conf/data-archiver.json", "Path to the configuration file")
)

func main() {
	flag.Parse()

	if *config == "" {
		flag.Usage()
		os.Exit(1)
	}

	conf, err := loadConfig(*config)
	if err != nil {
		log.Fatalf("Error loading config: %v\n", err.Error())
	}

	err = conf.Validate()
	if err != nil {
		log.Fatalf("Invalid config: %v\n", err.Error())
	}

	var (
		rsp ResourcesProvider
	)

	// Resource Catalog "mode"
	if conf.RC != nil {
		rsp, err = NewRCResourcesProvider(conf.RC)
	} else if conf.MR != nil {
		// Model Repository "mode"
		rsp, err = NewMRResourcesProvider(conf.MR)
	}

	if err != nil {
		log.Fatalf("Error intializing Resources Provider: %v\n", err.Error())
	}

	// Configure the HDS publisher
	hdsPublisher, err := NewHDSPublisher(conf.HDS, rsp, conf.MQTT)
	if err != nil {
		log.Fatalf("Error creating HDS publisher: %v\n", err.Error())
	}

	// Start the publisher
	err = hdsPublisher.Start()
	if err != nil {
		log.Fatalf("Error starting HDS publisher: %v\n", err.Error())
	}

	// sleep and wait for interrupt
	// Ctrl+C handling
	handler := make(chan os.Signal, 1)
	signal.Notify(handler, os.Interrupt, os.Kill)

	<-handler // block
	fmt.Println("Shutting down...")

	// Shutdown the HDS Publisher
	hdsPublisher.Shutdown()

	os.Exit(0)
}
