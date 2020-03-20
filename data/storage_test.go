package data

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
)

const (
	TOTALSERIES  = 10000
	TOTALENTRIES = 100000
)

func deleteFile(path string) {
	// delete file
	var err = os.Remove(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			fmt.Println("Failed deleting file:", err)
		}
		return
	}

	fmt.Println("done deleting file")
}

func setupTest(funcName string) (filename string, disconnectFunc func() error, dataStorage Storage, regStorage registry.Storage, err error) {
	fileName := os.TempDir() + "/" + funcName
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SQLITE, DSN: fileName}}
	dataStorage, disconnectFunc, err = NewSqlStorage(dataConf)
	if err != nil {
		return "", nil, nil, nil, err
	}
	//create a registry storage
	regStorage = registry.NewMemoryStorage(common.RegConf{}, dataStorage)
	return
}

func TestAPI_Submit(t *testing.T) {
	//Setup for the testing
	funcName := "TestAPI_Submit"
	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
	if err != nil {
		t.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer func() {
		err := disconnectFunc()
		if err != nil {
			log.Fatal(err)
		}
	}()

	testFuncs := map[string]func(t *testing.T, storage Storage, regstorage registry.Storage){
		"InsertValues":    testInsertVals,
		"InsertStrings":   testInsertStrings,
		"InsertBools":     testInsertBools,
		"InsertData":      testInsertData,
		"InsertMultiType": testInsertMultiType,
	}

	for k, testFunc := range testFuncs {
		t.Run(k, func(t *testing.T) {
			fmt.Printf("\n%s:", k)
			testFunc(t, dataStorage, regStorage)
		})
	}
}

func testInsertMultiType(t *testing.T, storage Storage, regstorage registry.Storage) {
	totRec := 101
	//Float Type
	streamMap := map[string]*registry.DataStream{
		"Value/Temperature": {Name: "Value/Temperature", Type: registry.Float, Unit: "Cel"},
		"Value/Room":        {Name: "Value/Room", Type: registry.String},
		"Value/Switch":      {Name: "Value/Switch", Type: registry.Bool},
		"Value/Camera":      {Name: "Value/Camera", Type: registry.Data},
	}

	sentDataMap := make(map[string]senml.Pack)
	streamArr := make([]*registry.DataStream, 0, len(streamMap))
	for _, stream := range streamMap {
		_, err := regstorage.Add(*stream)
		if err != nil {
			t.Fatal("Insertion failed:", err)
		}
		sentDataMap[stream.Name] = Same_name_same_types(totRec, *stream, true)
		streamArr = append(streamArr, stream)
	}

	defer func() {
		for name, _ := range streamMap {
			err := regstorage.Delete(name)
			if err != nil {
				t.Fatal("deletion failed:", err)
			}
		}
	}()
	err := storage.Submit(sentDataMap, streamMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.Query(Query{Sort: common.Desc, Denormalize: FName | FTime, count: true, To: time.Now().UTC(), PerPage: totRec * 4}, streamArr...)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec*4 {
		t.Errorf("Received count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec*4 {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	gotrecords.Normalize()

	//normalize sent records
	for i, _ := range sentDataMap {
		sentDataMap[i].Normalize()
	}

	for i := 0; i < 4; i++ { //This loop runs under the assumption that the senml records are ordered according to names
		start := totRec * i
		if CompareSenml(gotrecords[start:start+totRec], sentDataMap[gotrecords[start].Name]) == false {
			t.Error("Sent records and received record did not match!!")
		}
	}

}

func testInsertData(t *testing.T, storage Storage, regstorage registry.Storage) {
	ds := registry.DataStream{Name: "Value/Camera", Type: registry.Data}
	_, err := regstorage.Add(ds)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regstorage.Delete(ds.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ds, true)
	streamMap := make(map[string]*registry.DataStream)
	streamMap[ds.Name] = &ds
	recordMap := make(map[string]senml.Pack)
	recordMap[ds.Name] = sentData
	err = storage.Submit(recordMap, streamMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.Query(Query{Sort: common.Desc, Denormalize: FName | FTime, count: true, To: time.Now().UTC(), PerPage: totRec}, &ds)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	sentData.Normalize()
	gotrecords.Normalize()
	if CompareSenml(gotrecords, sentData) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func testInsertBools(t *testing.T, storage Storage, regstorage registry.Storage) {
	ds := registry.DataStream{Name: "Value/Switch", Type: registry.Float, Unit: "Cel"}
	_, err := regstorage.Add(ds)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regstorage.Delete(ds.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ds, true)
	streamMap := make(map[string]*registry.DataStream)
	streamMap[ds.Name] = &ds
	recordMap := make(map[string]senml.Pack)
	recordMap[ds.Name] = sentData
	err = storage.Submit(recordMap, streamMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.Query(Query{Sort: common.Desc, Denormalize: FName | FTime, count: true, To: time.Now().UTC(), PerPage: totRec}, &ds)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	sentData.Normalize()
	gotrecords.Normalize()
	if CompareSenml(gotrecords, sentData) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func testInsertStrings(t *testing.T, storage Storage, regstorage registry.Storage) {
	ds := registry.DataStream{Name: "Value/Room", Type: registry.String}
	_, err := regstorage.Add(ds)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regstorage.Delete(ds.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ds, true)
	streamMap := make(map[string]*registry.DataStream)
	streamMap[ds.Name] = &ds
	recordMap := make(map[string]senml.Pack)
	recordMap[ds.Name] = sentData
	err = storage.Submit(recordMap, streamMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.Query(Query{Sort: common.Desc, Denormalize: FName | FTime, count: true, To: time.Now().UTC(), PerPage: totRec}, &ds)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	sentData.Normalize()
	gotrecords.Normalize()
	if CompareSenml(gotrecords, sentData) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func testInsertVals(t *testing.T, storage Storage, regstorage registry.Storage) {
	ds := registry.DataStream{Name: "Value/temperature", Type: registry.Float, Unit: "Cel"}
	_, err := regstorage.Add(ds)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regstorage.Delete(ds.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ds, true)
	streamMap := make(map[string]*registry.DataStream)
	streamMap[ds.Name] = &ds
	recordMap := make(map[string]senml.Pack)
	recordMap[ds.Name] = sentData
	err = storage.Submit(recordMap, streamMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.Query(Query{Sort: common.Desc, Denormalize: FName | FTime, count: true, To: time.Now().UTC(), PerPage: totRec}, &ds)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	sentData.Normalize()
	gotrecords.Normalize()
	if CompareSenml(gotrecords, sentData) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func BenchmarkCreation_OneSeries(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	funcName := "BenchmarkCreation_OneSeries"

	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer func() {
		err := disconnectFunc()
		if err != nil {
			log.Fatal(err)
		}
	}()

	datastream := registry.DataStream{Name: funcName, Type: registry.Float}

	_, err = regStorage.Add(datastream)
	if err != nil {
		b.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := b.N
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	records = Same_name_same_types(totRec, datastream, true)
	recordMap := make(map[string]senml.Pack)
	recordMap[datastream.Name] = records
	streamMap := make(map[string]*registry.DataStream)
	streamMap[datastream.Name] = &datastream
	b.StartTimer()
	err = dataStorage.Submit(recordMap, streamMap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed", err)
	}

}

func BenchmarkCreation_OneSeriesTestGroup(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	funcName := "BenchmarkCreation_OneSeries"
	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer func() {
		err := disconnectFunc()
		if err != nil {
			log.Fatal(err)
		}
	}()

	//Actual benchmarking
	datastream := registry.DataStream{Name: funcName, Type: registry.Float}
	_, err = regStorage.Add(datastream)
	if err != nil {
		b.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := TOTALENTRIES
	records = Same_name_same_types(totRec, datastream, true)

	recordMap := make(map[string]senml.Pack)
	recordMap[datastream.Name] = records
	streamMap := make(map[string]*registry.DataStream)
	streamMap[datastream.Name] = &datastream
	err = dataStorage.Submit(recordMap, streamMap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed:", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, timeStart float64, timeEnd float64, stream *registry.DataStream){
		"InsertEnd":    benchmarkInsertEnd,
		"InsertRandom": benchmarkInsertRandom,
		"QueryRandom":  benchmarkQueryRandom,
		//"Getseries":       benchmarkQuerySeries,
	}

	startTime := records[len(records)-1].Time //since it is decremental
	endTime := records[0].Time

	b.StartTimer()
	for k, bm := range benchmarks {
		b.Run(k, func(b *testing.B) {
			fmt.Printf("\n%s:Count = %d\n", k, b.N)
			bm(b, dataStorage, startTime, endTime, &datastream)
		})
	}

}

func benchmarkInsertEnd(b *testing.B, storage Storage, _ float64, timeEnd float64, stream *registry.DataStream) {
	endTime := timeEnd
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		endTime := endTime + 1
		insRecords := Same_name_same_types(1, *stream, true)
		insRecords[0].Time = endTime
		recordMap := make(map[string]senml.Pack)
		recordMap[stream.Name] = insRecords
		streamMap := make(map[string]*registry.DataStream)
		streamMap[stream.Name] = stream
		b.StartTimer()
		err := storage.Submit(recordMap, streamMap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}
func benchmarkInsertRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, stream *registry.DataStream) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		randTime := between(timeStart, timeEnd)
		insRecords := Same_name_same_types(1, *stream, true)
		insRecords[0].Time = randTime
		recordMap := make(map[string]senml.Pack)
		recordMap[stream.Name] = insRecords
		streamMap := make(map[string]*registry.DataStream)
		streamMap[stream.Name] = stream
		b.StartTimer()
		err := storage.Submit(recordMap, streamMap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}

func benchmarkQueryRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, stream *registry.DataStream) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		start := between(timeStart, timeEnd)
		_, _, err := storage.Query(Query{From: time.Unix(0, int64(start*(1e9))), To: time.Unix(0, int64((start+2.0)*(1e9)))}, &registry.DataStream{Name: stream.Name})
		if err != nil {
			b.Error("query failed", err)
		}
	}
}
func BenchmarkCreation_MultiSeries(b *testing.B) {
	//Setup for the testing
	b.StopTimer()
	funcName := "BenchmarkCreation_MultiSeries"
	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer func() {
		err := disconnectFunc()
		if err != nil {
			log.Fatal(err)
		}
	}()

	datastream := registry.DataStream{Name: fileName, Type: registry.Float}

	// send some data
	var records senml.Pack
	totRec := 3
	records = Same_name_same_types(totRec, datastream, true)

	recordmap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < b.N; i++ {
		datastream.Name = strconv.Itoa(i)
		records[0].BaseName = datastream.Name
		_, err := regStorage.Add(datastream)
		if err != nil {
			b.Fatal("Error adding datastream:", err)
		}
		recordmap[datastream.Name] = records
		streamMap[datastream.Name] = &datastream
	}
	b.StartTimer()
	err = dataStorage.Submit(recordmap, streamMap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_MultiSeriesTestGroup(b *testing.B) {
	//Setup for the testing
	funcName := "BenchmarkCreation_MultiSeriesTestGroup"
	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer func() {
		err := disconnectFunc()
		if err != nil {
			log.Fatal(err)
		}
	}()
	// send some data
	var records senml.Pack
	totRec := 1
	records = Same_name_same_types(totRec, registry.DataStream{Name: "dummy", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, TOTALSERIES)
	streamMap := make(map[string]*registry.DataStream, TOTALSERIES)
	//fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < TOTALSERIES; i++ {
		datastream := registry.DataStream{Name: strconv.Itoa(i), Type: registry.Float}
		_, err := regStorage.Add(datastream)
		if err != nil {
			b.Fatal("Error adding datastream:", err)
		}
		newRecords := make(senml.Pack, totRec)
		copy(newRecords, records)
		newRecords[0].BaseName = datastream.Name
		recordMap[datastream.Name] = newRecords
		streamMap[datastream.Name] = &datastream
	}
	err = dataStorage.Submit(recordMap, streamMap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Fatal("Insetion failed", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, regStorage registry.Storage){
		"CreateNewSeries": benchmarkCreateNewSeries,
		"DeleteSeries":    benchmarkDeleteSeries,
		"Getseries":       benchmarkQuerySeries,
	}

	for k, bm := range benchmarks {
		fmt.Println("Main")

		ok := b.Run(k, func(b *testing.B) {
			fmt.Printf("\nStarting %s:Count = %d\n", k, b.N)
			bm(b, dataStorage, regStorage)
			fmt.Printf("\nDone %s:Count = %d\n", k, b.N)
		})
		if !ok {
			b.Error("Failed for ", k)
			break
		}
	}

}

func benchmarkCreateNewSeries(b *testing.B, storage Storage, regStorage registry.Storage) {
	records := Same_name_same_types(1, registry.DataStream{Name: "benchmarkCreateNewSeries", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		_, err := regStorage.Add(datastream)
		if err != nil {
			b.Fatal("Error adding datastream:", err)
		}
		newRecords := make(senml.Pack, 1)
		copy(newRecords, records)
		newRecords[0].BaseName = datastream.Name
		recordMap[datastream.Name] = newRecords
		streamMap[datastream.Name] = &datastream
	}
	b.StartTimer()
	err := storage.Submit(recordMap, streamMap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}

}

func benchmarkDeleteSeries(b *testing.B, storage Storage, regStorage registry.Storage) {
	b.StopTimer()
	totRec := 1
	records := Same_name_same_types(totRec, registry.DataStream{Name: "benchmarkDeleteSeries", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		_, err := regStorage.Add(datastream)
		if err != nil {
			b.Fatal("Error adding datastream:", err)
		}
		newrecords := make(senml.Pack, totRec)
		copy(newrecords, records)
		newrecords[0].BaseName = datastream.Name
		recordMap[datastream.Name] = newrecords
		streamMap[datastream.Name] = &datastream
	}

	err := storage.Submit(recordMap, streamMap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		err := storage.DeleteHandler(datastream)
		if err != nil {
			b.Fatal("Error deleting:", err)
		}
	}
}

func benchmarkQuerySeries(b *testing.B, storage Storage, _ registry.Storage) {
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Query(Query{}, &registry.DataStream{Name: strconv.Itoa(i % TOTALSERIES)})
		if err != nil {
			b.Fatal("Error querying:", err)
		}
	}

}
