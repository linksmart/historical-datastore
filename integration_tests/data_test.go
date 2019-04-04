package integration_tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
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
	time := 1543059346.0
	for i := 0; i < totRec; i++ {
		v := float64(i)
		records = append(records, senml.Record{Name: datastream.Name, Value: &v, Time: time})
	}
	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Error(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{}, datastream.Name)
	if err != nil {
		t.Fatal(err)
	}
	if len(gotrecords.Data) != 1 {
		t.Error("Received total should be 1 instead of ", len(gotrecords.Data))
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
	totRec := 10
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{Sort: common.DESC}, datastream.Name)
	if err != nil {
		t.Error(err)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}

	if common.CompareSenml(gotrecords.Data, records.Normalize()) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func TestInsertRandom(t *testing.T) {
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
	totRec := 10
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//create a randomly insertable record and insert to the datastore
	insrecords := common.Same_name_same_types(1, datastream.Name, true)
	insrecords[0].Time = insrecords[0].Time + float64(rand.Intn(totRec-1)) + 0.5
	fmt.Println(insrecords[0].Time)
	b, _ = json.Marshal(insrecords)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	totRec = totRec + 1
	records = records.Normalize()

	//insert one point randomly
	// send some data
	var inspos int
	insrecord := insrecords.Normalize()[0]
	for inspos = range records {
		if records[inspos].Time < insrecord.Time {
			break
		}
	}
	effREcords := append(records, senml.Record{})
	copy(effREcords[inspos+1:], effREcords[inspos:])
	effREcords[inspos] = insrecord

	//get these data
	gotrecords, err := dataClient.Query(data.Query{Sort: common.DESC}, datastream.Name)
	if err != nil {
		t.Error(err)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}

	fmt.Print("gotrecords: \n")
	printstr, _ := gotrecords.Data.Encode(senml.JSON, senml.OutputOptions{})
	fmt.Println(string(printstr))
	fmt.Print("effRecords: \n")
	printstr, _ = effREcords.Encode(senml.JSON, senml.OutputOptions{})
	fmt.Println(string(printstr))
	fmt.Print("records: \n")
	printstr, _ = records.Encode(senml.JSON, senml.OutputOptions{})
	fmt.Println(string(printstr))

	if common.CompareSenml(gotrecords.Data, effREcords.Normalize()) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
