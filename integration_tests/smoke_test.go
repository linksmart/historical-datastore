package integration_tests

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"code.linksmart.eu/hds/historical-datastore/data"

	"code.linksmart.eu/hds/historical-datastore/registry"
	"github.com/krylovsk/gosenml"
)

const endpoint = "http://localhost:8085"
const registryEndpoint = endpoint + "/registry"
const dataEndpoint = endpoint + "/data"

func TestConcurrentDeletes(t *testing.T) {
	registryClient, err := registry.NewRemoteClient(registryEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dataClient, err := data.NewRemoteClient(dataEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}

	dsJSON := []byte(`{
            "resource": "",
            "retention": "",
            "aggregation": [
                {
                    "interval": "1h",
                    "aggregates": [
                        "max",
                        "min"
                    ],
                    "retention": ""
                },
                {
                    "interval": "1h",
                    "aggregates": [
                        "mean",
                        "stddev"
                    ],
                    "retention": ""
                }
            ],
            "type": "float",
            "format": "application/senml+json"
        }`)

	var ds registry.DataSource
	err = json.Unmarshal(dsJSON, &ds)
	if err != nil {
		t.Fatal(err)
	}

	// create many
	for i := 0; i < 10; i++ {
		ds.Resource = fmt.Sprintf("dummy/%d", i)

		_, err = registryClient.Add(&ds)
		if err != nil {
			t.Log(err)
		}
	}

	// retrieve them
	reg, err := registryClient.Index(1, 100)
	if err != nil {
		t.Fatal(err)
	}

	// send some data
	for _, ds := range reg.Entries {
		var senml gosenml.Message
		for i := 0; i < 100; i++ {
			v := float64(i)
			senml.Entries = append(senml.Entries, gosenml.Entry{Name: ds.Resource, Value: &v})
		}
		b, _ := json.Marshal(senml)
		err := dataClient.Submit(b, "application/senml+json", ds.ID)
		if err != nil {
			t.Log(err)
		}
	}

	// delete all concurrently
	var wg sync.WaitGroup
	for _, ds := range reg.Entries {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := registryClient.Delete(id)
			if err != nil {
				t.Fatal(err)
			}
		}(ds.ID)
	}

	wg.Wait()
}
