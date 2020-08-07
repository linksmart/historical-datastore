package data

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
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

func setupTest(funcName string) (filename string, disconnectFunc func() error, dataStorage Storage, regController registry.Controller, err error) {
	fileName := os.TempDir() + "/" + funcName

	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SQLITE, DSN: fileName}}
	dataStorage, disconnectFunc, err = NewSqlStorage(dataConf)
	if err != nil {
		return "", nil, nil, registry.Controller{}, err
	}
	//create a registry storage
	regStorage := registry.NewMemoryStorage(common.RegConf{}, dataStorage)
	regController = *registry.NewController(regStorage)
	return
}

func TestStorage_Submit(t *testing.T) {
	//Setup for the testing
	funcName := "TestStorage_Submit"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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

	testFuncs := map[string]func(t *testing.T, storage Storage, regController registry.Controller){
		"InsertValues":    testInsertVals,
		"InsertStrings":   testInsertStrings,
		"InsertBools":     testInsertBools,
		"InsertData":      testInsertData,
		"InsertMultiType": testInsertMultiType,
	}

	for k, testFunc := range testFuncs {
		t.Run(k, func(t *testing.T) {
			fmt.Printf("\n%s:", k)
			testFunc(t, dataStorage, regController)
		})
	}
}

func testInsertMultiType(t *testing.T, storage Storage, regController registry.Controller) {
	totRec := 101
	//Float Type
	seriesMap := map[string]*registry.TimeSeries{
		"Value/Temperature": {Name: "Value/Temperature", Type: registry.Float, Unit: "Cel"},
		"Value/Room":        {Name: "Value/Room", Type: registry.String},
		"Value/Switch":      {Name: "Value/Switch", Type: registry.Bool},
		"Value/Camera":      {Name: "Value/Camera", Type: registry.Data},
	}

	sentDataMap := make(map[string]senml.Pack)
	seriesArr := make([]*registry.TimeSeries, 0, len(seriesMap))
	for _, series := range seriesMap {
		_, err := regController.Add(*series)
		if err != nil {
			t.Fatal("Insertion failed:", err)
		}
		sentDataMap[series.Name] = Same_name_same_types(totRec, *series, true)
		seriesArr = append(seriesArr, series)
	}

	defer func() {
		for name, _ := range seriesMap {
			err := regController.Delete(name)
			if err != nil {
				t.Fatal("deletion failed:", err)
			}
		}
	}()
	ctx := context.Background()
	err := storage.Submit(ctx, sentDataMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec * 4}, seriesArr...)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec*4 {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != totRec*4 {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	gotrecords.Normalize()
	// segregate the got records
	gotDataMap := make(map[string]senml.Pack)

	for _, record := range gotrecords {
		gotDataMap[record.Name] = append(gotDataMap[record.Name], record)
	}

	// normalize sent records
	for i, _ := range sentDataMap {
		sentDataMap[i].Normalize()
		if CompareSenml(gotDataMap[i], sentDataMap[i]) == false {
			t.Error("Sent records and received record did not match!!")
		}
	}

}

func testInsertData(t *testing.T, storage Storage, regController registry.Controller) {
	ts := registry.TimeSeries{Name: "Value/Camera", Type: registry.Data}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ts, true)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData
	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec}, &ts)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
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

func testInsertBools(t *testing.T, storage Storage, regController registry.Controller) {
	ts := registry.TimeSeries{Name: "Value/Switch", Type: registry.Float, Unit: "Cel"}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ts, true)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData

	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec}, &ts)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
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

func testInsertStrings(t *testing.T, storage Storage, regController registry.Controller) {
	ts := registry.TimeSeries{Name: "Value/Room", Type: registry.String}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ts, true)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData
	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotrecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec}, &ts)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
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

func testInsertVals(t *testing.T, storage Storage, regController registry.Controller) {
	ts := registry.TimeSeries{Name: "Value/temperature", Type: registry.Float, Unit: "Cel"}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ts, true)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData

	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	//get these data
	gotRecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec}, &ts)
	if err != nil {
		t.Error(err)
	}

	if *total != totRec {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotRecords) != totRec {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotRecords))
	}

	sentData.Normalize()
	gotRecords.Normalize()
	if CompareSenml(gotRecords, sentData) == false {
		t.Error("Sent records and received record did not match!!")
	}
}

func TestStorage_Aggregation(t *testing.T) {
	//Setup for the testing
	funcName := "TestStorage_Aggregation"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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

	testFuncs := map[string]func(t *testing.T, storage Storage, regController registry.Controller, aggr string){
		"aggrSingleSeries":   testAggSingleSeries,
		"aggrMultipleSeries": testAggMultipleSeries,
	}

	aggr := []string{"mean", "sum", "min", "max", "count"}

	for k, testFunc := range testFuncs {
		for _, a := range aggr {
			testName := k + "_" + a
			t.Run(testName, func(t *testing.T) {
				fmt.Printf("\n%s", testName)
				testFunc(t, dataStorage, regController, a)
			})
		}
	}

}
func getAggrFunction(aggr string) aggrFunction {
	switch aggr {
	case "mean":
		return aggrAvg
	case "count":
		return aggrCount
	case "max":
		return aggrMax
	case "min":
		return aggrMin
	case "sum":
		return aggrSum
	default:
		return nil
	}
}

func testAggMultipleSeries(t *testing.T, storage Storage, regController registry.Controller, aggr string) {

	seriesMap := map[string]*registry.TimeSeries{
		"Bedroom/Temperature": {Name: "Bedroom/Temperature", Type: registry.Float, Unit: "Cel"},
		"Hall/Temperature":    {Name: "Hall/Temperature", Type: registry.Float, Unit: "Cel"},
		"Kitchen/Temperature": {Name: "Kitchen/Temperature", Type: registry.Float, Unit: "Cel"},
		"Balcony/Temperature": {Name: "Balcony/Temperature", Type: registry.Float, Unit: "Cel"},
	}

	seriesArr := make([]*registry.TimeSeries, 0, len(seriesMap))
	for _, series := range seriesMap {
		_, err := regController.Add(*series)
		if err != nil {
			t.Fatal("Insertion failed:", err)
		}
		seriesArr = append(seriesArr, series)
	}

	defer func() {
		for name, _ := range seriesMap {
			err := regController.Delete(name)
			if err != nil {
				t.Fatal("deletion failed:", err)
			}
		}
	}()
	sentData, expectedData := sampleDataForAggregation(5, 1594000000, 1594100000, getAggrFunction(aggr), 5*time.Minute, seriesArr...)
	sentDataMap := make(map[string]senml.Pack)
	for _, r := range sentData {
		sentDataMap[r.Name] = append(sentDataMap[r.Name], r)
	}
	ctx := context.Background()
	err := storage.Submit(ctx, sentDataMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}
	expectedLen := int(math.Min(float64(len(expectedData)), MaxPerPage))
	//get these data

	gotRecords, total, err := storage.QueryPage(ctx, Query{Count: true,
		To:         fromSenmlTime(sentData[len(sentData)-1].Time),
		From:       fromSenmlTime(sentData[0].Time),
		Page:       1,
		PerPage:    expectedLen,
		SortAsc:    true,
		AggrFunc:   aggr,
		AggrWindow: 5 * time.Minute}, seriesArr...)
	if err != nil {
		t.Error(err)
		return
	}

	if *total != len(expectedData) {
		t.Errorf("Received total count should be %d, got %d instead", len(expectedData), *total)
		return
	}

	if len(gotRecords) != expectedLen {
		t.Errorf("Received record length should be %d, got %d (len) instead", len(expectedData), len(gotRecords))
		return
	}

	expectedDataMap := make(map[string]senml.Pack)
	for _, r := range expectedData[0:expectedLen] {
		expectedDataMap[r.Name] = append(expectedDataMap[r.Name], r)
	}
	gotRecordsMap := make(map[string]senml.Pack)
	for _, r := range gotRecords {
		gotRecordsMap[r.Name] = append(gotRecordsMap[r.Name], r)
	}
	for key, _ := range expectedDataMap {
		if CompareSenml(gotRecordsMap[key], expectedDataMap[key]) == false {
			t.Error("Sent records and expected records did not match!!")
		}
	}
}

func testAggSingleSeries(t *testing.T, storage Storage, regController registry.Controller, aggr string) {
	ts := registry.TimeSeries{Name: "Value/temperature", Type: registry.Float, Unit: "Cel"}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err = regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()

	sentData, expectedData := sampleDataForAggregation(5, 1594000000, 1594100000, getAggrFunction(aggr), 5*time.Minute, &ts)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData
	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	expectedLen := int(math.Min(float64(len(expectedData)), MaxPerPage))
	//get these data

	gotRecords, total, err := storage.QueryPage(ctx, Query{Count: true,
		To:         fromSenmlTime(sentData[len(sentData)-1].Time),
		From:       fromSenmlTime(sentData[0].Time),
		Page:       1,
		PerPage:    expectedLen,
		SortAsc:    true,
		AggrFunc:   aggr,
		AggrWindow: 5 * time.Minute}, &ts)
	if err != nil {
		t.Error(err)
		return
	}

	if *total != len(expectedData) {
		t.Errorf("Received total count should be %d, got %d instead", len(expectedData), *total)
		return
	}

	if len(gotRecords) != expectedLen {
		t.Errorf("Received record length should be %d, got %d (len) instead", len(expectedData), len(gotRecords))
		return
	}

	if CompareSenml(gotRecords, expectedData[0:expectedLen]) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
func TestStorage_Delete(t *testing.T) {
	//Setup for the testing
	funcName := "TestStorage_Delete"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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

	testFuncs := map[string]func(t *testing.T, storage Storage, regController registry.Controller){
		"DeleteValues":    testDeleteVals,
		"DeleteMultiType": testDeleteMultiType,
	}

	for k, testFunc := range testFuncs {
		t.Run(k, func(t *testing.T) {
			fmt.Printf("\n%s:", k)
			testFunc(t, dataStorage, regController)
		})
	}

}

func testDeleteMultiType(t *testing.T, storage Storage, regController registry.Controller) {
	totRec := 101
	//Float Type
	seriesMap := map[string]*registry.TimeSeries{
		"Value/Temperature": {Name: "Value/Temperature", Type: registry.Float, Unit: "Cel"},
		"Value/Room":        {Name: "Value/Room", Type: registry.String},
		"Value/Switch":      {Name: "Value/Switch", Type: registry.Bool},
		"Value/Camera":      {Name: "Value/Camera", Type: registry.Data},
	}

	sentDataMap := make(map[string]senml.Pack)
	seriesArr := make([]*registry.TimeSeries, 0, len(seriesMap))
	for _, series := range seriesMap {
		_, err := regController.Add(*series)
		if err != nil {
			t.Fatal("Insertion failed:", err)
		}
		sentDataMap[series.Name] = Same_name_same_types(totRec, *series, true)
		seriesArr = append(seriesArr, series)
	}

	defer func() {
		for name, _ := range seriesMap {
			err := regController.Delete(name)
			if err != nil {
				t.Fatal("deletion failed:", err)
			}
		}
	}()
	ctx := context.Background()
	err := storage.Submit(ctx, sentDataMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	delCount := 50
	toTime := fromSenmlTime(sentDataMap["Value/Temperature"][delCount].Time)
	//get these data
	err = storage.Delete(ctx, seriesArr, time.Time{}, toTime)
	if err != nil {
		t.Error(err)
	}

	seriesCount := len(seriesMap)

	//get these data
	gotrecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec * 4}, seriesArr...)
	if err != nil {
		t.Error(err)
	}

	if *total != delCount*seriesCount {
		t.Errorf("Received Count should be %d, got %d (len) instead", totRec, *total)
	}
	if len(gotrecords) != *total {
		t.Errorf("Received total should be %d, got %d (len) instead", totRec, len(gotrecords))
	}

	gotrecords.Normalize()
	// segregate the got records
	gotDataMap := make(map[string]senml.Pack)

	for _, record := range gotrecords {
		gotDataMap[record.Name] = append(gotDataMap[record.Name], record)
	}

	// normalize sent records
	for i, _ := range sentDataMap {
		sentDataMap[i].Normalize()
		if CompareSenml(gotDataMap[i], sentDataMap[i][0:delCount]) == false {
			t.Error("Sent records and received record did not match!!")
		}
	}
}

func testDeleteVals(t *testing.T, storage Storage, regController registry.Controller) {
	ts := registry.TimeSeries{Name: "Value/temperature", Type: registry.Float, Unit: "Cel"}
	var err error
	_, err = regController.Add(ts)
	if err != nil {
		t.Fatal("Insertion failed:", err)
	}
	defer func() {
		err := regController.Delete(ts.Name)
		if err != nil {
			t.Fatal("deletion failed:", err)
		}
	}()
	totRec := 101
	sentData := Same_name_same_types(totRec, ts, true)
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[ts.Name] = &ts
	recordMap := make(map[string]senml.Pack)
	recordMap[ts.Name] = sentData
	ctx := context.Background()
	err = storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		t.Error("Error while inserting:", err)
	}

	delCount := 50
	toTime := fromSenmlTime(sentData[delCount].Time)

	err = storage.Delete(ctx, []*registry.TimeSeries{&ts}, time.Time{}, toTime)
	if err != nil {
		t.Error(err)
		return
	}

	gotRecords, total, err := storage.QueryPage(ctx, Query{Denormalize: DenormMaskName | DenormMaskTime, Count: true, To: time.Now().UTC(), PerPage: totRec}, &ts)
	if err != nil {
		t.Error(err)
		return
	}

	if *total != delCount {
		t.Errorf("Received Count should be %d, got %d (len) instead", delCount, *total)
		return
	}

	sentData.Normalize()
	gotRecords.Normalize()
	if CompareSenml(gotRecords, sentData[0:delCount]) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
func BenchmarkCreation_OneSeries(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	funcName := "BenchmarkCreation_OneSeries"

	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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

	series := registry.TimeSeries{Name: funcName, Type: registry.Float}

	_, addErr := regController.Add(series)
	if addErr != nil {
		b.Fatal(addErr)
	}

	// send some data
	var records senml.Pack
	totRec := b.N
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	records = Same_name_same_types(totRec, series, true)
	recordMap := make(map[string]senml.Pack)
	recordMap[series.Name] = records
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[series.Name] = &series
	b.StartTimer()
	ctx := context.Background()
	err = dataStorage.Submit(ctx, recordMap, seriesMap)
	//err = dataClient.Submit(barr, , series.Name)
	if err != nil {
		b.Error("Insetion failed", err)
	}

}

func BenchmarkCreation_OneSeriesTestGroup(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	funcName := "BenchmarkCreation_OneSeries"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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
	series := registry.TimeSeries{Name: funcName, Type: registry.Float}
	_, err = regController.Add(series)
	if err != nil {
		b.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := TOTALENTRIES
	records = Same_name_same_types(totRec, series, true)

	recordMap := make(map[string]senml.Pack)
	recordMap[series.Name] = records
	seriesMap := make(map[string]*registry.TimeSeries)
	seriesMap[series.Name] = &series
	ctx := context.Background()
	err = dataStorage.Submit(ctx, recordMap, seriesMap)
	//err = dataClient.Submit(barr, , series.Name)
	if err != nil {
		b.Error("Insetion failed:", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, timeStart float64, timeEnd float64, series *registry.TimeSeries){
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
			bm(b, dataStorage, startTime, endTime, &series)
		})
	}

}

func benchmarkInsertEnd(b *testing.B, storage Storage, _ float64, timeEnd float64, series *registry.TimeSeries) {
	endTime := timeEnd
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		endTime := endTime + 1
		insRecords := Same_name_same_types(1, *series, true)
		insRecords[0].Time = endTime
		recordMap := make(map[string]senml.Pack)
		recordMap[series.Name] = insRecords
		seriesMap := make(map[string]*registry.TimeSeries)
		seriesMap[series.Name] = series
		b.StartTimer()
		ctx := context.Background()
		err := storage.Submit(ctx, recordMap, seriesMap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}
func benchmarkInsertRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, series *registry.TimeSeries) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		randTime := between(timeStart, timeEnd)
		insRecords := Same_name_same_types(1, *series, true)
		insRecords[0].Time = randTime
		recordMap := make(map[string]senml.Pack)
		recordMap[series.Name] = insRecords
		seriesMap := make(map[string]*registry.TimeSeries)
		seriesMap[series.Name] = series
		b.StartTimer()
		ctx := context.Background()
		err := storage.Submit(ctx, recordMap, seriesMap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}

func benchmarkQueryRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, series *registry.TimeSeries) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		start := between(timeStart, timeEnd)
		ctx := context.Background()
		_, _, err := storage.QueryPage(ctx, Query{From: time.Unix(0, int64(start*(1e9))), To: time.Unix(0, int64((start+2.0)*(1e9)))}, &registry.TimeSeries{Name: series.Name})
		if err != nil {
			b.Error("query failed", err)
		}
	}
}
func BenchmarkCreation_MultiSeries(b *testing.B) {
	//Setup for the testing
	b.StopTimer()
	funcName := "BenchmarkCreation_MultiSeries"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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

	series := registry.TimeSeries{Name: fileName, Type: registry.Float}

	// send some data
	var records senml.Pack
	totRec := 3
	records = Same_name_same_types(totRec, series, true)

	recordmap := make(map[string]senml.Pack, b.N)
	seriesMap := make(map[string]*registry.TimeSeries, b.N)
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < b.N; i++ {
		series.Name = strconv.Itoa(i)
		records[0].BaseName = series.Name
		_, err := regController.Add(series)
		if err != nil {
			b.Fatal("Error adding series:", err)
		}
		recordmap[series.Name] = records
		seriesMap[series.Name] = &series
	}
	b.StartTimer()
	ctx := context.Background()
	err = dataStorage.Submit(ctx, recordmap, seriesMap)
	//err = dataClient.Submit(barr, , series.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_MultiSeriesTestGroup(b *testing.B) {
	//Setup for the testing
	funcName := "BenchmarkCreation_MultiSeriesTestGroup"
	fileName, disconnectFunc, dataStorage, regController, err := setupTest(funcName)
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
	records = Same_name_same_types(totRec, registry.TimeSeries{Name: "dummy", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, TOTALSERIES)
	seriesMap := make(map[string]*registry.TimeSeries, TOTALSERIES)
	//fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < TOTALSERIES; i++ {
		series := registry.TimeSeries{Name: strconv.Itoa(i), Type: registry.Float}
		_, err := regController.Add(series)
		if err != nil {
			b.Fatal("Error adding series:", err)
		}
		newRecords := make(senml.Pack, totRec)
		copy(newRecords, records)
		newRecords[0].BaseName = series.Name
		recordMap[series.Name] = newRecords
		seriesMap[series.Name] = &series
	}
	ctx := context.Background()
	err = dataStorage.Submit(ctx, recordMap, seriesMap)
	//err = dataClient.Submit(barr, , stream.Name)
	if err != nil {
		b.Fatal("Insetion failed", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, regController registry.Controller){
		"CreateNewSeries": benchmarkCreateNewSeries,
		"DeleteSeries":    benchmarkDeleteSeries,
		"Getseries":       benchmarkQuerySeries,
	}

	for k, bm := range benchmarks {
		fmt.Println("Main")

		ok := b.Run(k, func(b *testing.B) {
			fmt.Printf("\nStarting %s:Count = %d\n", k, b.N)
			bm(b, dataStorage, regController)
			fmt.Printf("\nDone %s:Count = %d\n", k, b.N)
		})
		if !ok {
			b.Error("Failed for ", k)
			break
		}
	}

}

func benchmarkCreateNewSeries(b *testing.B, storage Storage, regController registry.Controller) {
	records := Same_name_same_types(1, registry.TimeSeries{Name: "benchmarkCreateNewSeries", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, b.N)
	seriesMap := make(map[string]*registry.TimeSeries, b.N)
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		series := registry.TimeSeries{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		_, err := regController.Add(series)
		if err != nil {
			b.Fatal("Error adding series:", err)
		}
		newRecords := make(senml.Pack, 1)
		copy(newRecords, records)
		newRecords[0].BaseName = series.Name
		recordMap[series.Name] = newRecords
		seriesMap[series.Name] = &series
	}
	b.StartTimer()
	ctx := context.Background()
	err := storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}

}

func benchmarkDeleteSeries(b *testing.B, storage Storage, regController registry.Controller) {
	b.StopTimer()
	totRec := 1
	records := Same_name_same_types(totRec, registry.TimeSeries{Name: "benchmarkDeleteSeries", Type: registry.Float, Unit: ""}, true)

	recordMap := make(map[string]senml.Pack, b.N)
	seriesMap := make(map[string]*registry.TimeSeries, b.N)
	for i := 0; i < b.N; i++ {
		series := registry.TimeSeries{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		_, err := regController.Add(series)
		if err != nil {
			b.Fatal("Error adding series:", err)
		}
		newrecords := make(senml.Pack, totRec)
		copy(newrecords, records)
		newrecords[0].BaseName = series.Name
		recordMap[series.Name] = newrecords
		seriesMap[series.Name] = &series
	}
	ctx := context.Background()
	err := storage.Submit(ctx, recordMap, seriesMap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		series := registry.TimeSeries{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
		err := storage.DeleteHandler(series)
		if err != nil {
			b.Fatal("Error deleting:", err)
		}
	}
}

func benchmarkQuerySeries(b *testing.B, storage Storage, _ registry.Controller) {
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		_, _, err := storage.QueryPage(ctx, Query{}, &registry.TimeSeries{Name: strconv.Itoa(i % TOTALSERIES)})
		if err != nil {
			b.Fatal("Error querying:", err)
		}
	}

}

type aggrFunction func(pack senml.Pack) senml.Pack

func aggrAvg(pack senml.Pack) senml.Pack {
	var retRec senml.Pack
	sumMap := make(map[string]float64)
	lenMap := make(map[string]int)
	for _, record := range pack {
		sumMap[record.Name] += *record.Value
		lenMap[record.Name] += 1
	}
	for k, v := range sumMap {
		avg := v / float64(lenMap[k])
		retRec = append(retRec, senml.Record{Name: k, Value: &avg, Time: pack[len(pack)-1].Time})
	}
	return retRec
}
func aggrSum(pack senml.Pack) senml.Pack {
	var retRec senml.Pack
	sumMap := make(map[string]float64)
	for _, record := range pack {
		sumMap[record.Name] += *record.Value
	}
	for k, v := range sumMap {
		sum := v
		retRec = append(retRec, senml.Record{Name: k, Value: &sum, Time: pack[len(pack)-1].Time})
	}
	return retRec
}

func aggrMin(pack senml.Pack) senml.Pack {
	var retRec senml.Pack
	packMap := make(map[string]senml.Pack)
	for _, record := range pack {
		packMap[record.Name] = append(packMap[record.Name], record)
	}

	for k, p := range packMap {
		var min float64
		for i, r := range p {
			if i == 0 || *r.Value < min {
				min = *r.Value
			}
		}
		retRec = append(retRec, senml.Record{Name: k, Value: &min, Time: pack[len(pack)-1].Time})
	}
	return retRec
}

func aggrMax(pack senml.Pack) senml.Pack {
	var retRec senml.Pack
	packMap := make(map[string]senml.Pack)
	for _, record := range pack {
		packMap[record.Name] = append(packMap[record.Name], record)
	}

	for k, p := range packMap {
		var max float64
		for i, r := range p {
			if i == 0 || *r.Value > max {
				max = *r.Value
			}
		}
		retRec = append(retRec, senml.Record{Name: k, Value: &max, Time: pack[len(pack)-1].Time})
	}
	return retRec
}

func aggrCount(pack senml.Pack) senml.Pack {
	var retRec senml.Pack
	lenMap := make(map[string]int)
	for _, record := range pack {
		lenMap[record.Name] += 1
	}
	for k, v := range lenMap {
		count := float64(v)
		retRec = append(retRec, senml.Record{Name: k, Value: &count, Time: pack[len(pack)-1].Time})
	}
	return retRec
}

func sampleDataForAggregation(maxPerBlock int,
	from float64,
	to float64,
	aggrFunc aggrFunction,
	interval time.Duration, series ...*registry.TimeSeries) (rawPack senml.Pack, aggrPack senml.Pack) {

	curVal := 0.0

	durSec := to - from
	intervalDurSec := interval.Seconds()
	increment := durSec / (intervalDurSec * float64(maxPerBlock))
	totalBlocks := int(durSec / intervalDurSec)
	totalCount := totalBlocks * maxPerBlock
	rawPack = make([]senml.Record, 0, totalCount)
	aggrPack = make([]senml.Record, 0, totalBlocks)

	for curTime := from; curTime < to; curTime += intervalDurSec {
		curPack := make([]senml.Record, 0, maxPerBlock)
		for i := curTime; i < curTime+intervalDurSec; i += increment {
			value := curVal
			for _, ts := range series {
				record := senml.Record{Name: ts.Name, Value: &value, Time: (i)}
				curPack = append(curPack, record)
			}
			curVal++

		}
		rawPack = append(rawPack, curPack...)
		aggrPack = append(aggrPack, aggrFunc(curPack)...)
	}

	return rawPack, aggrPack

}
