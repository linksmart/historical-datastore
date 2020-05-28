package data

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	data "github.com/linksmart/historical-datastore/data/proto"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func setupGrpcAPI(t *testing.T) (grpcClient *GrpcClient) {
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

	//grpcClient, err := newGrpcClientFromBufConListener(lis)
	bufDialer := func(ctx context.Context, s string) (conn net.Conn, err error) {
		return lis.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial bufnet: %v", err)
	}
	client := data.NewDataClient(conn)
	if err != nil {
		t.Fatalf("Unable to connect to server:%v", err)
		return nil
	}

	t.Cleanup(func() {
		conn.Close()
		api.StopGrpcServer()
	})
	return &GrpcClient{Client: client}
}

func TestGrpcSubmit(t *testing.T) {
	client := setupGrpcAPI(t)

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
