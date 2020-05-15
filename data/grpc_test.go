package data

import (
	"fmt"
	"log"
	"testing"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc/test/bufconn"
)

func setupGrpCAPI() (grpcClient *GrpcClient) {
	regStorage := registry.NewMemoryStorage(common.RegConf{})

	// Create three dummy datastreams with different types
	dss := []registry.DataStream{
		{
			Name: "http://example.com/sensor1",
			Unit: "degC",
			Type: registry.Float,
		},
		{
			Name: "http://example.com/sensor2",
			Unit: "flag",
			Type: registry.Bool,
		},
		{
			Name: "http://example.com/sensor3",
			Unit: "char",
			Type: registry.String,
		},
	}
	for _, ds := range dss {
		_, err := regStorage.Add(ds)
		if err != nil {
			fmt.Println("Error creating dummy DS:", err)
			break
		}
	}

	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	//start the server
	api := NewGrpcAPI(regStorage, &dummyDataStorage{}, false)

	go func() {
		if err := api.StartGrpcServer(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	grpcClient, err := NewBufferClient(lis)
	if err != nil {
		log.Fatalf("Unable to connect to server:%v", err)
	}
	return grpcClient
}
func TestGrpcSubmit(t *testing.T) {
	client := setupGrpCAPI()

	v1 := 42.0
	r1 := senml.Record{
		Name:  "example.com/sensor1",
		Unit:  "degC",
		Value: &v1,
	}
	v2 := true
	r2 := senml.Record{
		Name:      "example.com/sensor2",
		Unit:      "flag",
		BoolValue: &v2,
	}
	v3 := "test string"
	r3 := senml.Record{
		Name:        "example.com/sensor3",
		Unit:        "char",
		StringValue: v3,
	}
	r1.BaseName = "http://"
	records := []senml.Record{r1, r2, r3}
	err := client.Submit(records)
	if err != nil {
		t.Fatalf("Submit failed:%v", err)
	}

}
