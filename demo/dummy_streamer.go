package demo

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/farshidtz/senml"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
)

func StartDummyStreamer(regStorage registry.Storage, dataStorage data.Storage) error {
	dsBool, err := createDS(regStorage, "kitchen/lamp", "bool")
	if err != nil {
		return fmt.Errorf("error creating stream: %s", err)
	}
	dsString, err := createDS(regStorage, "hall/cat", "string")
	if err != nil {
		return fmt.Errorf("error creating stream: %s", err)
	}

	dsFloat, err := createDS(regStorage, "terrace/temperature", "float")
	if err != nil {
		return fmt.Errorf("error creating stream: %s", err)
	}

	streamDummyData := func() {
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			addFloat(dataStorage, dsFloat)
			addBool(dataStorage, dsBool)
			addString(dataStorage, dsString)
		}
	}

	go streamDummyData()

	return nil
}

func createDS(regStorage registry.Storage, name string, datatype string) (ds registry.DataStream, err error) {
	ds = registry.DataStream{
		Name: name,
		Type: datatype,
	}
	_, err = regStorage.Add(ds)
	if err != nil {
		if errors.Is(err, registry.ErrConflict) { // strings.HasPrefix(err.Error(), registry.ErrConflict.Error()) {
			log.Printf("Reusing existing stream %s", name)
		} else {
			log.Printf("Error creating datastream %s: %s", name, err)
			return ds, err
		}
	} else {
		log.Printf("Creating stream %s\n", ds.Name)
	}
	return ds, nil
}
func addFloat(datastorage data.Storage, ds registry.DataStream) {
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
		Name:  ds.Name,
		Value: &curVal,
	}

	log.Printf("Submitting %s: value %f\n", ds.Name, curVal)
	submitData(datastorage, ds.Name, senmlRecord)

}

func addBool(datastorage data.Storage, ds registry.DataStream) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var curVal bool = r.Intn(2) != 0
	senmlRecord := senml.Record{
		Name:      ds.Name,
		BoolValue: &curVal,
	}

	log.Printf("Submitting %s: value %t\n", ds.Name, curVal)
	submitData(datastorage, ds.Name, senmlRecord)

}

func addString(datastorage data.Storage, ds registry.DataStream) {
	status := []string{
		"Relaxed",
		"Stretching",
		"Yawning",
		"Cautious",
		"Tense",
		"Anxious",
		"Fearful",
		"Confident",
		"Grooming itself",
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	index := r.Intn(len(status))
	senmlRecord := senml.Record{
		Name:        ds.Name,
		StringValue: status[index],
	}
	log.Printf("Submitting %s: status %s", ds.Name, status[index])
	submitData(datastorage, ds.Name, senmlRecord)

}

func submitData(datastorage data.Storage, name string, record senml.Record) {
	var senmlPack senml.Pack = []senml.Record{record}
	recordmap := make(map[string]senml.Pack)
	recordmap[name] = senmlPack

	err := datastorage.Submit(recordmap)
	if err != nil {
		log.Printf("insetion failed: %s", err)
	}
}
