package data

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/common"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"code.linksmart.eu/hds/historical-datastore/senmltest"
	"github.com/farshidtz/senml"
)

func deleteFile(path string) {
	// delete file
	var err = os.Remove(path)
	if err != nil {
		fmt.Println("failed deleting file:", err)
		return
	}

	fmt.Println("done deleting file")
}

func BenchmarkCreation_OneSeries(b *testing.B) {
	//Setup for the testing
	fileName := os.TempDir() + "/BenchmarkCreation_OneSeries"
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
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)

	b.ResetTimer()
	registrymap := make(map[string]*registry.DataStream)
	registrymap[datastream.Name] = &datastream
	recordmap := make(map[string]senml.Pack)
	recordmap[datastream.Name] = records
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_MultiSeries(b *testing.B) {
	//Setup for the testing
	fileName := os.TempDir() + "BenchmarkCreation_MultiSeries"
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
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < b.N; i++ {
		datastream.Name = strconv.Itoa(i)
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = records
	}
	b.ResetTimer()
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

}

func BenchmarkCreation_MultiSeriesTestGroup(b *testing.B) {
	//Setup for the testing
	fileName := os.TempDir() + "BenchmarkCreation_MultiSeries"
	totalRegistries := 10000
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
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	//fmt.Printf("%s:Count = %d\n", fileName, b.N)
	for i := 0; i < totalRegistries; i++ {
		datastream.Name = strconv.Itoa(i)
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = records
	}
	err = dataStorage.Submit(recordmap, registrymap)
	//err = dataClient.Submit(barr, , datastream.Name)
	if err != nil {
		b.Error("Insetion failed")
	}

	benchmarks := map[string]func(b *testing.B, storage Storage, stream registry.DataStream, records senml.Pack){
		"CreateNewSeries": benchmarkCreateNewSeries,
		"DeleteSeries":    benchmarkDeleteSeries,
		"Getseries":       benchmarkQuerySeries,
	}

	for k, bm := range benchmarks {
		fmt.Println("Main")

		b.Run(k, func(b *testing.B) {
			fmt.Printf("\n%s:Count = %d\n", "benchmarkCreateNewSeries", b.N)
			for i := 0; i < b.N; i++ {
				bm(b, dataStorage, datastream, records)
			}
		})
	}

}

func benchmarkCreateNewSeries(b *testing.B, storage Storage, datastream registry.DataStream, records senml.Pack) {
	records = senmltest.Same_name_same_types(3, datastream.Name, true)
	datastream = registry.DataStream{Name: "", Type: common.FLOAT}
	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	for i := 0; i < b.N; i++ {
		datastream.Name = "new" + strconv.Itoa(b.N) + strconv.Itoa(i)
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = records
		storage.Submit(recordmap, registrymap)
	}

}

func benchmarkDeleteSeries(b *testing.B, storage Storage, datastream registry.DataStream, records senml.Pack) {
	records = senmltest.Same_name_same_types(3, datastream.Name, true)
	datastream = registry.DataStream{Name: "", Type: common.FLOAT}
	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	for i := 0; i < b.N; i++ {
		datastream.Name = "new" + strconv.Itoa(b.N) + strconv.Itoa(i)
		registrymap[datastream.Name] = &datastream
		recordmap[datastream.Name] = records
		storage.Submit(recordmap, registrymap)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		datastream.Name = "new" + strconv.Itoa(b.N) + strconv.Itoa(i)
		storage.DeleteHandler(datastream)
	}
}

func benchmarkQuerySeries(b *testing.B, storage Storage, datastream registry.DataStream, records senml.Pack) {

}
