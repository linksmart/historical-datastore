package integration_tests

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
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
		Type: registry.Float,
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
		Unit: "A",
		Type: registry.Float,
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
	records = data.Same_name_same_types(totRec, *datastream, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{}, datastream.Name)
	if err != nil {
		t.Error(err)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}
	records.Normalize()
	if data.CompareSenml(gotrecords.Data, records) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func TestCreationDiffTimestamp_Denormalized(t *testing.T) {
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
		Unit: "A",
		Type: registry.Float,
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
	records = data.Same_name_same_types(totRec, *datastream, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//get these data
	gotrecords, err := dataClient.Query(data.Query{ Denormalize: data.DenormMaskName | data.DenormMaskTime}, datastream.Name)
	if err != nil {
		t.Error(err)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}

	records.Normalize()
	gotrecords.Data.Normalize()
	if data.CompareSenml(gotrecords.Data, records) == false {
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
		Unit: "A",
		Type: registry.Float,
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
	records = data.Same_name_same_types(totRec, *datastream, true)

	b, _ := json.Marshal(records)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	//create a randomly insertable record and insert to the datastore
	insrecords := data.Same_name_same_types(1, *datastream, true)
	insrecords[0].Time = insrecords[0].Time + float64(rand.Intn(totRec-1)) + 0.5
	fmt.Println(insrecords[0].Time)
	b, _ = json.Marshal(insrecords)
	err = dataClient.Submit(b, "application/senml+json", datastream.Name)
	if err != nil {
		t.Fatal(err)
	}

	totRec = totRec + 1
	records.Normalize()

	//insert one point randomly
	// send some data
	var inspos int
	insrecords.Normalize()
	insrecord := insrecords[0]
	for inspos = range records {
		if records[inspos].Time < insrecord.Time {
			break
		}
	}
	effREcords := append(records, senml.Record{})
	copy(effREcords[inspos+1:], effREcords[inspos:])
	effREcords[inspos] = insrecord

	//get these data
	gotrecords, err := dataClient.Query(data.Query{}, datastream.Name)
	if err != nil {
		t.Error(err)
	}

	if len(gotrecords.Data) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords.Data))
	}

	fmt.Print("gotrecords: \n")
	printstr, _ := codec.EncodeCSV(gotrecords.Data, codec.SetDefaultHeader)
	fmt.Println(string(printstr))
	fmt.Print("effRecords: \n")
	printstr, _ = codec.EncodeCSV(effREcords, codec.SetDefaultHeader)
	fmt.Println(string(printstr))
	fmt.Print("records: \n")
	printstr, _ = codec.EncodeCSV(records, codec.SetDefaultHeader)
	fmt.Println(string(printstr))

	effREcords.Normalize()
	if data.CompareSenml(gotrecords.Data, effREcords) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
