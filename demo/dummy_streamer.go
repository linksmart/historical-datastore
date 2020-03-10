package demo

import (
	"log"
	"math/rand"
	"time"

	"github.com/farshidtz/senml"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
)

func DummyStreamer(regStorage registry.Storage, dataStorage data.Storage) {
	dsBool := createDS(regStorage, "kitchen/lamp", "bool")
	dsString := createDS(regStorage, "hall/cat", "string")
	dsFloat := createDS(regStorage, "terrace/temperature", "float")

	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		addFloat(dataStorage, dsFloat)
		addBool(dataStorage, dsBool)
		addString(dataStorage, dsString)
	}

}

func createDS(regStorage registry.Storage, name string, datatype string) registry.DataStream {
	ds := registry.DataStream{
		Name: name,
		Type: datatype,
	}
	_, err := regStorage.Add(ds)
	if err != nil {
		log.Printf("Error creating datastream %s: %s", name, err)
	}
	log.Printf("Creating stream %s\n", ds.Name)
	return ds
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
	submitData(datastorage, ds, senmlRecord)

}

func addBool(datastorage data.Storage, ds registry.DataStream) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var curVal bool = r.Intn(2) != 0
	senmlRecord := senml.Record{
		Name:      ds.Name,
		BoolValue: &curVal,
	}

	log.Printf("Submitting %s: value %t\n", ds.Name, curVal)
	submitData(datastorage, ds, senmlRecord)

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
	submitData(datastorage, ds, senmlRecord)

}

func submitData(datastorage data.Storage, ds registry.DataStream, record senml.Record) {
	var senmlPack senml.Pack = []senml.Record{record}
	recordMap := make(map[string]senml.Pack)
	senmlPack = senmlPack.Normalize()
	recordMap[ds.Name] = senmlPack
	streamMap := make(map[string]*registry.DataStream)
	streamMap[ds.Name] = &ds
	err := datastorage.Submit(recordMap, streamMap)
	if err != nil {
		log.Printf("insetion failed: %s", err)
	}
}
