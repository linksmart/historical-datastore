package demo

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/farshidtz/senml"
	"github.com/linksmart/historical-datastore/registry"
)

const maxRetryCount = 5

var hdsURL string

func DummyStreamer(server string) {

	flag.Parse()
	log.Println("Started the Dummy Generator")

	hdsURL = server

	dsBool := postDS("kitchen/lamp", "bool")
	dsString := postDS("hall/cat", "string")
	dsFloat := postDS("terrace/temperature", "float")

	ticker := time.NewTicker(time.Second * 5)
	for range ticker.C {
		sendFloat(dsFloat)
		sendBool(dsBool)
		sendString(dsString)
	}

}

func postDS(name string, datatype string) registry.DataStream {
	ds := registry.DataStream{
		Name: name,
		Type: datatype,
	}
	jsonValue, _ := json.Marshal(ds)
	retryCount := 0
	for {
		if retryCount > maxRetryCount {
			break
		}
		log.Println("Looking for the existing datastream with name", name)
		resp, err := http.Get(hdsURL + "/registry/one/name/suffix/" + name)
		if err != nil {
			log.Printf("Error: %s. \n Retrying...", err)
			time.Sleep(2 * time.Second)
			retryCount++
			continue
		}
		if resp.StatusCode == 200 {
			body, _ := ioutil.ReadAll(resp.Body)
			var reg registry.DataStreamList
			err := json.Unmarshal(body, &reg)
			if err != nil {
				log.Fatalf("Error: %s", err)
			}
			if reg.Total != 0 {
				log.Println("Found datastream", reg.Streams[0].Name)
				break
			}
		} else {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Fatalf("%s: %s", resp.Status, string(body))
		}

		log.Println("Creating datastream named", name)
		resp, err = http.Post(hdsURL+"/registry", "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			log.Printf("Error: %s. Retrying...", err)
			time.Sleep(1 * time.Second)
			retryCount++
			continue
		}
		if resp.StatusCode == 201 {
			location, err := resp.Location()
			if err != nil {
				log.Fatalln(err)
			}
			locationVal := strings.Split(location.Path, "/")[2]
			log.Println("Created datasource", locationVal)
			break
		} else if resp.StatusCode == 409 {
			log.Println("Resource already created, continuing..")
			break
		} else {
			body, _ := ioutil.ReadAll(resp.Body)
			log.Printf("%s: %s. Retrying...", resp.Status, string(body))
			time.Sleep(1 * time.Second)
			retryCount++
			continue
		}
	}

	return ds
}
func sendFloat(ds registry.DataStream) {
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

	submitData(ds.Name, senmlRecord)

}

func sendBool(ds registry.DataStream) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var curVal bool = r.Intn(2) != 0
	senmlRecord := senml.Record{
		Name:      ds.Name,
		BoolValue: &curVal,
	}

	submitData(ds.Name, senmlRecord)

}

func sendString(ds registry.DataStream) {
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

	submitData(ds.Name, senmlRecord)

}

func submitData(name string, record senml.Record) {
	var senmlPack senml.Pack = []senml.Record{record}
	jsonValue, _ := senmlPack.Encode(senml.JSON, senml.OutputOptions{})
	log.Println("Submitting", string(jsonValue))
	resp, err := http.Post(hdsURL+"/data/"+name, "application/senml+json", bytes.NewBuffer(jsonValue))
	if err != nil {
		log.Printf("Error: %s", err)
	} else if resp.StatusCode != 202 {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Printf("%s: %s", resp.Status, string(body))
	}
}
