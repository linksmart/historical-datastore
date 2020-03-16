package data

import (
	"errors"
	"fmt"
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

func setupBenchmark(funcName string) (filename string, disconnectFunc func() error, dataStorage Storage, regStorage registry.Storage, err error) {
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
func BenchmarkCreation_OneSeries(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	funcName := "BenchmarkCreation_OneSeries"

	fileName, disconnectFunc, dataStorage, regStorage, err := setupBenchmark(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer disconnectFunc()

	datastream := registry.DataStream{Name: funcName, Type: registry.Float}

	_, err = regStorage.Add(datastream)
	if err != nil {
		b.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := b.N
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	records = common.Same_name_same_types(totRec, datastream.Name, true)
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
	fileName, disconnectFunc, dataStorage, regStorage, err := setupBenchmark(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer disconnectFunc()

	//Actual benchmarking
	datastream := registry.DataStream{Name: funcName, Type: registry.Float}
	_, err = regStorage.Add(datastream)
	if err != nil {
		b.Fatal(err)
	}

	// send some data
	var records senml.Pack
	totRec := TOTALENTRIES
	records = common.Same_name_same_types(totRec, datastream.Name, true)

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

func benchmarkInsertEnd(b *testing.B, storage Storage, timeStart float64, timeEnd float64, stream *registry.DataStream) {
	endTime := timeEnd
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		endTime := endTime + 1
		insRecords := common.Same_name_same_types(1, stream.Name, true)
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
		insRecords := common.Same_name_same_types(1, stream.Name, true)
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
	fileName, disconnectFunc, dataStorage, regStorage, err := setupBenchmark(funcName)
	if err != nil {
		b.Fatalf("Error setting up benchmark:%s", err)
	}
	defer deleteFile(fileName)
	defer disconnectFunc()

	datastream := registry.DataStream{Name: fileName, Type: registry.Float}

	// send some data
	var records senml.Pack
	totRec := 3
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	recordmap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < b.N; i++ {
		datastream.Name = strconv.Itoa(i)
		records[0].BaseName = datastream.Name
		regStorage.Add(datastream)
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
	fileName := os.TempDir() + "BenchmarkCreation_MultiSeries"
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SQLITE, DSN: fileName}}
	dataStorage, disconnect_func, err := NewSqlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer deleteFile(fileName)
	defer disconnect_func()

	// send some data
	var records senml.Pack
	totRec := 1
	records = common.Same_name_same_types(totRec, "dummy", true)

	recordMap := make(map[string]senml.Pack, TOTALSERIES)
	streamMap := make(map[string]*registry.DataStream, TOTALSERIES)
	//fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < TOTALSERIES; i++ {
		datastream := registry.DataStream{Name: strconv.Itoa(i), Type: registry.Float}
		newrecords := make(senml.Pack, totRec)
		copy(newrecords, records)
		newrecords[0].BaseName = datastream.Name
		recordMap[datastream.Name] = newrecords
		streamMap[datastream.Name] = &datastream
	}
	err = dataStorage.Submit(recordMap, streamMap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Fatal("Insetion failed", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage){
		"CreateNewSeries": benchmarkCreateNewSeries,
		"DeleteSeries":    benchmarkDeleteSeries,
		"Getseries":       benchmarkQuerySeries,
	}

	for k, bm := range benchmarks {
		fmt.Println("Main")

		ok := b.Run(k, func(b *testing.B) {
			fmt.Printf("\nStarting %s:Count = %d\n", k, b.N)
			bm(b, dataStorage)
			fmt.Printf("\nDone %s:Count = %d\n", k, b.N)
		})
		if !ok {
			b.Error("Failed for ", k)
			break
		}
	}

}

func benchmarkCreateNewSeries(b *testing.B, storage Storage) {
	records := common.Same_name_same_types(1, "benchmarkCreateNewSeries", true)

	recordMap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
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

func benchmarkDeleteSeries(b *testing.B, storage Storage) {
	b.StopTimer()
	totRec := 1
	records := common.Same_name_same_types(totRec, "benchmarkDeleteSeries", true)

	recordMap := make(map[string]senml.Pack, b.N)
	streamMap := make(map[string]*registry.DataStream, b.N)
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: registry.Float}
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

func benchmarkQuerySeries(b *testing.B, storage Storage) {
	for i := 0; i < b.N; i++ {
		_, _, err := storage.Query(Query{}, &registry.DataStream{Name: strconv.Itoa(i % TOTALSERIES)})
		if err != nil {
			b.Fatal("Error querying:", err)
		}
	}

}
