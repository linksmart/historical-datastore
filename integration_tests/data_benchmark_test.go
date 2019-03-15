package integration_tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/registry"
	"code.linksmart.eu/hds/historical-datastore/senmltest"
	"github.com/farshidtz/senml"
	uuid "github.com/satori/go.uuid"
)

func BenchmarkCreation_SameTimestamp(b *testing.B) {
	//funcName := "TestCreation_SameTimestamp"
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		b.Fatal(err)
	}

	datastream := &registry.DataStream{
		Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
		Type: "float",
	}

	fmt.Printf("Creating the datastream with ID %s\n", datastream.Name)
	_, err = registryClient.Add(datastream)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		fmt.Println("Deleting the datastream")
		err = registryClient.Delete(datastream.Name)
		if err != nil {
			b.Fatal(err)
		}
	}()
	// send some data
	// send some data
	var records senml.Pack
	totRec := b.N
	fmt.Printf("Count = %d\n", b.N)
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)
	barr, err := json.Marshal(records)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	res, err := http.Post(dataEndpoint+"/a"+datastream.Name, "application/senml+json", bytes.NewReader(barr))
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Fatal(err)
	}
	if res.StatusCode != http.StatusAccepted {
		b.Fatalf("Got response %v", res.StatusCode)
	}

}
