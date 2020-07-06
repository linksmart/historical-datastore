package data

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/farshidtz/senml/v2"
	data "github.com/linksmart/historical-datastore/data/proto"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func setupGrpcAPI(t *testing.T, dataStorage Storage, regStorage registry.Storage) (grpcClient *GrpcClient) {
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
	api := NewGrpcAPI(regStorage, dataStorage, false)

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
	funcName := "TestGrpcSubmit"
	fileName, disconnectFunc, dataStorage, regStorage, err := setupTest(funcName)
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
	client := setupGrpcAPI(t, dataStorage, regStorage)

	v1 := 42.0
	r1 := senml.Record{
		Name:  "example.com/sensor1",
		Unit:  "degC",
		Value: &v1,
		Time:  1543059346.0,
	}
	v2 := true
	r2 := senml.Record{
		Name:      "example.com/sensor2",
		Unit:      "flag",
		BoolValue: &v2,
		Time:      1543059346.0,
	}
	v3 := "test string"
	r3 := senml.Record{
		Name:        "example.com/sensor3",
		Unit:        "char",
		StringValue: v3,
		Time:        1543059346.0,
	}
	r1.BaseName = "http://"
	records := senml.Pack{r1, r2, r3}
	err = client.Submit(records)
	if err != nil {
		t.Fatalf("Submit failed:%v", err)
	}

	//validate submission by checking the count
	q := Query{To: time.Now()}
	streamNames := []string{"http://example.com/sensor1", "http://example.com/sensor2", "http://example.com/sensor3"}
	total, err := client.Count(streamNames, q)
	if err != nil {
		t.Errorf("Fetching count failed:%v", err)
	}
	if total != len(records) {
		t.Errorf("Returned total is not the expected value:%d", len(records))
	}

	//Query the values
	pack, err := client.Query(streamNames, q)
	if err != nil {
		t.Errorf("Query failed:%v", err)
	}

	records.Normalize()
	pack.Normalize()
	if CompareSenml(records, pack) == false {
		t.Error("Sent records and received record did not match!!")
	}
}
