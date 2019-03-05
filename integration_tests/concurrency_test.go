package integration_tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/data"
	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/farshidtz/senml"
	uuid "github.com/satori/go.uuid"
)

func TestConcurrentCreates(t *testing.T) {
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	var entries []*registry.DataStream

	// create many concurrently
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ds := &registry.DataStream{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: "float",
		}
		entries = append(entries, ds)
	}

	idCollector := make(chan string, TOTAL)
	var wg sync.WaitGroup
	for i := 0; i < TOTAL; i++ {
		wg.Add(1)
		go func(thisDS *registry.DataStream) {
			defer wg.Done()
			id, err := registryClient.Add(thisDS)
			if err != nil {
				t.Fatal(err)
			}
			idCollector <- id
		}(entries[i])
	}
	wg.Wait()
	close(idCollector)

	c := 0
	for id := range idCollector {
		_, err := registryClient.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		c++
	}
	if c != TOTAL {
		t.Fatalf("Total created %d instead of %d", c, TOTAL)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataClient, err := data.NewRemoteClient(dataEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	var entries []*registry.DataStream

	// create many
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ds := registry.DataStream{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: "float",
		}
		ds.Name, err = registryClient.Add(&ds)
		if err != nil {
			t.Fatal(err)
		}
		addedDS, err := registryClient.Get(ds.Name)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, addedDS)
	}

	// send some data
	for _, ds := range entries {
		var records []senml.Record
		for i := 0; i < 100; i++ {
			v := float64(i)
			records = append(records, senml.Record{Name: ds.Name, Value: &v})
		}
		b, _ := json.Marshal(records)
		err := dataClient.Submit(b, "application/senml+json", ds.Name)
		if err != nil {
			t.Fatal(err)
		}
	}

	// update all concurrently
	var wg sync.WaitGroup
	for i := 0; i < TOTAL; i++ {
		wg.Add(1)
		go func(thisDS *registry.DataStream) {
			defer wg.Done()
			thisDS.Retention.Min = ""
			err := registryClient.Update(thisDS.Name, thisDS)
			if err != nil {
				t.Fatal(err)
			}
		}(entries[i])
	}

	wg.Wait()
}

func TestConcurrentDeletes(t *testing.T) {
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataClient, err := data.NewRemoteClient(dataEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	var entries []registry.DataStream

	// create many
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ds := registry.DataStream{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: "float",
		}
		_, err = registryClient.Add(&ds)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, ds)
	}

	// send some data
	for _, ds := range entries {
		var records []senml.Record
		for i := 0; i < 100; i++ {
			v := float64(i)
			records = append(records, senml.Record{Name: ds.Name, Value: &v})
		}
		b, _ := json.Marshal(records)
		err := dataClient.Submit(b, "application/senml+json", ds.Name)
		if err != nil {
			t.Fatal(err)
		}
	}

	// delete all concurrently
	var wg sync.WaitGroup
	for i := 0; i < TOTAL; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := registryClient.Delete(id)
			if err != nil {
				t.Fatal(err)
			}
		}(entries[i].Name)
	}

	wg.Wait()
}
