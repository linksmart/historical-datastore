package data

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/farshidtz/senml"
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
		fmt.Println("failed deleting file (Ignore this error if it comes in the beginning):", err)
		return
	}

	fmt.Println("done deleting file")
}

func BenchmarkCreation_OneSeries(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	fileName := os.TempDir() + "/BenchmarkCreation_OneSeries"
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SENMLSTORE, DSN: fileName}}
	defer deleteFile(fileName)
	var err error
	if err != nil {
		b.Fatal(err)
	}
	dataStorage, disconnect_func, err := NewSenmlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer disconnect_func()

	//Actual benchmarking
	datastream := registry.DataStream{Name: fileName, Type: common.FLOAT}

	// send some data
	// send some data
	var records senml.Pack
	totRec := b.N
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	registrymap[datastream.Name] = &datastream
	recordmap := make(map[string]senml.Pack)
	recordmap[datastream.Name] = records
	b.StartTimer()
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_OneSeriesTestGroup(b *testing.B) {
	b.StopTimer()
	//Setup for the testing
	fileName := os.TempDir() + "/BenchmarkCreation_OneSeries"
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SENMLSTORE, DSN: fileName}}
	defer deleteFile(fileName)
	var err error
	if err != nil {
		b.Fatal(err)
	}
	dataStorage, disconnect_func, err := NewSenmlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer disconnect_func()

	//Actual benchmarking
	datastream := registry.DataStream{Name: fileName, Type: common.FLOAT}

	// send some data
	// send some data
	var records senml.Pack
	totRec := TOTALENTRIES
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	registrymap[datastream.Name] = &datastream
	recordmap := make(map[string]senml.Pack)
	recordmap[datastream.Name] = records
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed", err)
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, timeStart float64, timeEnd float64, seriesName string){
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
			bm(b, dataStorage, startTime, endTime, datastream.Name)
		})
	}

}

func benchmarkInsertEnd(b *testing.B, storage Storage, timeStart float64, timeEnd float64, seriesName string) {
	endTime := timeEnd
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		endTime := endTime + 1
		insrecords := common.Same_name_same_types(1, seriesName, true)
		insrecords[0].Time = endTime
		registrymap := make(map[string]*registry.DataStream)
		registrymap[seriesName] = &registry.DataStream{Name: seriesName}
		recordmap := make(map[string]senml.Pack)
		recordmap[seriesName] = insrecords
		b.StartTimer()
		err := storage.Submit(recordmap, registrymap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}
func benchmarkInsertRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, seriesName string) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		randTime := between(timeStart, timeEnd)
		insrecords := common.Same_name_same_types(1, seriesName, true)
		insrecords[0].Time = randTime
		registrymap := make(map[string]*registry.DataStream)
		registrymap[seriesName] = &registry.DataStream{Name: seriesName}
		recordmap := make(map[string]senml.Pack)
		recordmap[seriesName] = insrecords
		b.StartTimer()
		err := storage.Submit(recordmap, registrymap)
		if err != nil {
			b.Error("insetion failed", err)
		}
	}
}

func benchmarkQueryRandom(b *testing.B, storage Storage, timeStart float64, timeEnd float64, seriesName string) {
	between := func(min float64, max float64) (randNum float64) {
		return min + rand.Float64()*(max-min)
	}
	for i := 0; i < b.N; i++ {
		start := between(timeStart, timeEnd)
		_, _, _, err := storage.Query(Query{From: time.Unix(0, int64(start*(1e9))), To: time.Unix(0, int64((start+2.0)*(1e9)))}, &registry.DataStream{Name: seriesName})
		if err != nil {
			b.Error("query failed", err)
		}
	}
}
func BenchmarkCreation_MultiSeries(b *testing.B) {
	//Setup for the testing
	b.StopTimer()
	fileName := os.TempDir() + "BenchmarkCreation_MultiSeries"
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SENMLSTORE, DSN: fileName}}

	dataStorage, disconnect_func, err := NewSenmlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer deleteFile(fileName)
	defer disconnect_func()

	datastream := registry.DataStream{Name: fileName, Type: common.FLOAT}

	// send some data
	var records senml.Pack
	totRec := 3
	records = common.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < b.N; i++ {
		datastream.Name = strconv.Itoa(i)
		records[0].BaseName = datastream.Name
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = records
	}
	b.StartTimer()
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_MultiSeriesTestGroup(b *testing.B) {
	//Setup for the testing
	fileName := os.TempDir() + "BenchmarkCreation_MultiSeries"
	deleteFile(fileName)
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SENMLSTORE, DSN: fileName}}
	dataStorage, disconnect_func, err := NewSenmlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer deleteFile(fileName)
	defer disconnect_func()

	// send some data
	var records senml.Pack
	totRec := 1
	records = common.Same_name_same_types(totRec, "dummy", true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	//fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < TOTALSERIES; i++ {
		datastream := registry.DataStream{Name: strconv.Itoa(i), Type: common.FLOAT}
		newrecords := make(senml.Pack, totRec)
		copy(newrecords, records)
		newrecords[0].BaseName = datastream.Name
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = newrecords
	}
	err = dataStorage.Submit(recordmap, registrymap)
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

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: common.FLOAT}
		newRecords := make(senml.Pack, 1)
		copy(newRecords, records)
		newRecords[0].BaseName = datastream.Name
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = newRecords

	}
	b.StartTimer()
	err := storage.Submit(recordmap, registrymap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}

}

func benchmarkDeleteSeries(b *testing.B, storage Storage) {
	b.StopTimer()
	totRec := 1
	records := common.Same_name_same_types(totRec, "benchmarkDeleteSeries", true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: common.FLOAT}
		newrecords := make(senml.Pack, totRec)
		copy(newrecords, records)
		newrecords[0].BaseName = datastream.Name
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = newrecords
	}
	err := storage.Submit(recordmap, registrymap)
	if err != nil {
		b.Fatal("Error creating:", err)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		datastream := registry.DataStream{Name: "new" + strconv.Itoa(b.N) + strconv.Itoa(i), Type: common.FLOAT}
		err := storage.DeleteHandler(datastream)
		if err != nil {
			b.Fatal("Error deleting:", err)
		}
	}
}

func benchmarkQuerySeries(b *testing.B, storage Storage) {
	for i := 0; i < b.N; i++ {
		_, _, _, err := storage.Query(Query{}, &registry.DataStream{Name: strconv.Itoa(i % TOTALSERIES)})
		if err != nil {
			b.Fatal("Error querying:", err)
		}
	}

}
