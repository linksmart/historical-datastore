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

var (
	dataStorage *LightdbStorage
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

func BenchmarkCreation_SameTimestamp(b *testing.B) {
	//Setup for the testing
	fileName := os.TempDir() + "/BenchmarkCreation_SameTimestamp"
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

func BenchmarkCreation_Multiregistry(b *testing.B) {
	//Setup for the testing
	funcName := "BenchmarkCreation_Multiregistry"
	dataConf := common.DataConf{Backend: common.DataBackendConf{Type: SENMLSTORE, DSN: funcName}}
	defer deleteFile(funcName)
	var err error
	if err != nil {
		b.Fatal(err)
	}
	dataStorage, disconnect_func, err := NewSenmlStorage(dataConf)
	if err != nil {
		b.Fatal(err)
	}
	defer disconnect_func()

	datastream := registry.DataStream{Name: funcName, Type: common.FLOAT}

	// send some data
	var records senml.Pack
	totRec := 3
	records = senmltest.Same_name_same_types(totRec, datastream.Name, true)

	registrymap := make(map[string]*registry.DataStream)
	recordmap := make(map[string]senml.Pack)
	fmt.Printf("%s:Count = %d\n", funcName, b.N)
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
