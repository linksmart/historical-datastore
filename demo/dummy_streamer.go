package demo

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
)

func StartDummyStreamer(regController registry.Controller, dataController data.Controller) error {
	tsBool, err := createTS(regController, "kitchen/lamp", registry.Bool, "")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}
	tsString, err := createTS(regController, "hall/cat", registry.String, "")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}

	tsFloat, err := createTS(regController, "terrace/temperature", registry.Float, "Cel")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}

	streamDummyData := func() {
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			addFloat(dataController, tsFloat)
			addBool(dataController, tsBool)
			addString(dataController, tsString)
		}
	}

	go streamDummyData()

	return nil
}

func createTS(regController registry.Controller, name string, datatype registry.ValueType, unit string) (ts registry.TimeSeries, err error) {
	ts = registry.TimeSeries{
		Name: name,
		Type: datatype,
		Unit: unit,
	}
	_, err = regController.Add(ts)
	if err != nil {
		if _, ok := err.(*common.ConflictError); ok {
			log.Printf("Reusing existing time series %s", name)
		} else {
			log.Printf("Error creating time series %s: %s", name, err)
			return ts, err
		}
	} else {
		log.Printf("Creating time series %s\n", ts.Name)
	}
	return ts, nil
}
func addFloat(dataController data.Controller, ts registry.TimeSeries) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	curVal := 20.0
	max := 40.0
	min := 0.0
	chrange := 2.0
	chMin := -1.0
	rchange := r.Float64()*(chrange) + chMin
	if curVal+rchange <= max || curVal+rchange >= min {
		curVal += rchange
	}
	senmlRecord := senml.Record{
		Name:  ts.Name,
		Unit:  "Cel",
		Value: &curVal,
	}

	log.Printf("Submitting %s: value %f\n", ts.Name, curVal)
	submitData(dataController, ts, senmlRecord)

}

func addBool(dataController data.Controller, ts registry.TimeSeries) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var curVal bool = r.Intn(2) != 0
	senmlRecord := senml.Record{
		Name:      ts.Name,
		BoolValue: &curVal,
	}

	log.Printf("Submitting %s: value %t\n", ts.Name, curVal)
	submitData(dataController, ts, senmlRecord)

}

func addString(dataController data.Controller, ts registry.TimeSeries) {
	status := []string{
		"Relaxed",
		"Stretching",
		"Yawning",
		"Cautious",
		"Tense",
		"Anxious",
		"Fearful",
		"Hungry",
		"Grooming itself",
		//"Walking on the keyboard %&!§$%&//,())=?`{}[*':];\"",
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := r.Intn(len(status))
	senmlRecord := senml.Record{
		Name:        ts.Name,
		StringValue: status[index],
	}
	log.Printf("Submitting %s: status %s", ts.Name, status[index])
	submitData(dataController, ts, senmlRecord)

}

func submitData(dataController data.Controller, ts registry.TimeSeries, record senml.Record) {
	var senmlPack senml.Pack = []senml.Record{record}
	seriesList := []string{ts.Name}
	err := dataController.Submit(context.Background(), senmlPack, seriesList)
	if err != nil {
		log.Printf("insetion failed: %s", err)
	}
}
