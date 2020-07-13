package demo

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
)

func StartDummyStreamer(regStorage registry.Storage, dataStorage data.Storage) error {
	tsBool, err := createTS(regStorage, "kitchen/lamp", registry.Bool, "")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}
	tsString, err := createTS(regStorage, "hall/cat", registry.String, "")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}

	tsFloat, err := createTS(regStorage, "terrace/temperature", registry.Float, "Cel")
	if err != nil {
		return fmt.Errorf("error creating time series: %s", err)
	}

	streamDummyData := func() {
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			addFloat(dataStorage, tsFloat)
			addBool(dataStorage, tsBool)
			addString(dataStorage, tsString)
		}
	}

	go streamDummyData()

	return nil
}

func createTS(regStorage registry.Storage, name string, datatype registry.ValueType, unit string) (ts registry.TimeSeries, err error) {
	ts = registry.TimeSeries{
		Name: name,
		Type: datatype,
		Unit: unit,
	}
	_, err = regStorage.Add(ts)
	if err != nil {
		if errors.Is(err, registry.ErrConflict) { // strings.HasPrefix(err.Error(), registry.ErrConflict.Error()) {
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
func addFloat(datastorage data.Storage, ts registry.TimeSeries) {
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
	submitData(datastorage, ts, senmlRecord)

}

func addBool(datastorage data.Storage, ts registry.TimeSeries) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var curVal bool = r.Intn(2) != 0
	senmlRecord := senml.Record{
		Name:      ts.Name,
		BoolValue: &curVal,
	}

	log.Printf("Submitting %s: value %t\n", ts.Name, curVal)
	submitData(datastorage, ts, senmlRecord)

}

func addString(datastorage data.Storage, ts registry.TimeSeries) {
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
	submitData(datastorage, ts, senmlRecord)

}

func submitData(datastorage data.Storage, ts registry.TimeSeries, record senml.Record) {
	var senmlPack senml.Pack = []senml.Record{record}
	recordMap := make(map[string]senml.Pack)
	senmlPack.Normalize()
	recordMap[ts.Name] = senmlPack
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	err := datastorage.Submit(recordMap, seriesMap)
	if err != nil {
		log.Printf("insetion failed: %s", err)
	}
}
