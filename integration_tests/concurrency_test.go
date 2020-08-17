package integration_tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
	uuid "github.com/satori/go.uuid"
)

func TestConcurrentCreates(t *testing.T) {
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	var entries []*registry.TimeSeries

	// create many concurrently
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ts := &registry.TimeSeries{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: registry.Float,
		}
		entries = append(entries, ts)
	}

	var wg sync.WaitGroup
	for i := 0; i < TOTAL; i++ {
		wg.Add(1)
		go func(thisTS *registry.TimeSeries) {
			defer wg.Done()
			_, err := registryClient.Add(thisTS)
			if err != nil {
				t.Fatal(err)
			}
		}(entries[i])
	}
	wg.Wait()

	for i := 0; i < TOTAL; i++ {
		_, err := registryClient.Get(entries[i].Name)
		if err != nil {
			t.Fatal(err)
		}
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

	var entries []*registry.TimeSeries

	// create many
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ts := registry.TimeSeries{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: registry.Float,
		}
		_, err = registryClient.Add(&ts)
		if err != nil {
			t.Fatal(err)
		}
		addedTS, err := registryClient.Get(ts.Name)
		if err != nil {
			t.Error(err, ts.Name)
			return
		}
		entries = append(entries, addedTS)
	}

	// send some data
	for _, ts := range entries {
		var records []senml.Record
		for i := 0; i < 100; i++ {
			v := float64(i)
			records = append(records, senml.Record{Name: ts.Name, Value: &v})
		}
		b, _ := json.Marshal(records)
		err := dataClient.Submit(b, "application/senml+json", ts.Name)
		if err != nil {
			t.Fatal(err)
		}
	}

	// update all concurrently
	var wg sync.WaitGroup
	for i := 0; i < TOTAL; i++ {
		wg.Add(1)
		go func(thisTS *registry.TimeSeries) {
			defer wg.Done()
			err := registryClient.Update(thisTS.Name, thisTS)
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

	var entries []registry.TimeSeries

	// create many
	const TOTAL = 10
	for i := 0; i < TOTAL; i++ {
		ts := registry.TimeSeries{
			Name: fmt.Sprintf("dummy/%s", uuid.NewV4().String()),
			Type: registry.Float,
		}
		_, err = registryClient.Add(&ts)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, ts)
	}

	// send some data
	for _, ts := range entries {
		var records []senml.Record
		for i := 0; i < 100; i++ {
			v := float64(i)
			records = append(records, senml.Record{Name: ts.Name, Value: &v})
		}
		b, _ := json.Marshal(records)
		err := dataClient.Submit(b, "application/senml+json", ts.Name)
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

	for i := 0; i < TOTAL; i++ {
		{
			_, err := registryClient.Get(entries[i].Name)
			if !errors.Is(err, registry.ErrNotFound) {
				t.Fatal(err)
			}
		}
	}
}
