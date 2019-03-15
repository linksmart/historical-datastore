package integration_tests

import (
	"encoding/json"
	"fmt"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"code.linksmart.eu/hds/historical-datastore/senmltest"
	"github.com/farshidtz/senml"
	uuid "github.com/satori/go.uuid"
)

func TestCreationSameTimestamp(t *testing.T) {
	//funcName := "TestCreation_SameTimestamp"
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataClient, err := data.NewRemoteClient(dataEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	datastream := &registry.DataStream{
		Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
		Type: "float",
	}

	fmt.Printf("Creating the datastream with ID %s\n", datastream.Name)
	_, err = registryClient.Add(datastream)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fmt.Println("Deleting the datastream")
		err = registryClient.Delete(datastream.Name)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// send some data
	var records []senml.Record
	totRec := 10
	for i := 0; i < totRec; i++ {
		v := float64(i)
		records = append(records, senml.Record{Name: datastream.Name, Value: &v})
	}
	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Error(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{}, 1, totRec, datastream.Name)
	if err != nil {
		t.Error(err)
	}
	if gotrecords.Total != 1 {
		t.Error("Received total should be 1")
	}

	if len(gotrecords.Data) != 1 {
		t.Error("Received total should be 1")
	}

}

func TestCreationDiffTimestamp(t *testing.T) {
	//funcName := "TestCreation_diffTimestamp"
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataClient, err := data.NewRemoteClient(dataEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	datastream := &registry.DataStream{
		Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
		Type: "float",
	}

	defer func() {
		fmt.Printf("Deleting the datastream %s\n", datastream.Name)
		err = registryClient.Delete(datastream.Name)
		if err != nil {
			t.Fatal(err)
		}
	}()
	fmt.Printf("Creating the datastream with ID %s\n", datastream.Name)
	_, err = registryClient.Add(datastream)
	if err != nil {
		t.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := 1000
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{Sort: common.DESC}, 1, totRec, datastream.Name)
	if err != nil {
		t.Error(err)
	}
	if gotrecords.Total != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, gotrecords.Total)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}

	if senmltest.CompareSenml(gotrecords.Data, records.Normalize()) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
