package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"testing"
	"time"

	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func setupGrpcAPI(t *testing.T, regController Controller) (grpcClient *GrpcClient) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	//start the server
	srv := grpc.NewServer()
	RegisterGRPCAPI(srv, regController, false)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	bufDialer := func(ctx context.Context, s string) (conn net.Conn, err error) {
		return lis.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial bufnet: %v", err)
	}
	client := _go.NewRegistryClient(conn)
	if err != nil {
		t.Fatalf("Unable to connect to server:%v", err)
		return nil
	}

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})
	return &GrpcClient{Client: client}
}

func TestGrpcAPI_Add(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()
	controller := *NewController(storage)
	client := setupGrpcAPI(t, controller)

	var ts TimeSeries
	ts.Name = "any_url"
	//ts.Retention = ""
	//ts.Aggregation TODO
	ts.Type = String

	err = client.Add(ts)
	if err != nil {
		t.Fatalf("Received unexpected error on add: %v", err.Error())
	}

	getTS, err := client.Get(ts.Name)
	if err != nil {
		t.Fatalf("Received unexpected error on get: %v", err.Error())
	}

	// compare added and retrieved data
	addedBytes, _ := json.Marshal(&ts)
	getBytes, _ := json.Marshal(&getTS)
	if string(getBytes) != string(addedBytes) {
		t.Fatalf("Mismatch:\n added:\n%v\n retrieved:\n%v\n", string(addedBytes), string(getBytes))
	}
}

func TestGrpcAPI_Get(t *testing.T) {
	t.Skip("Tested in TestLevelDBAdd")
}

func insertDummyData(quantity int, client *GrpcClient) ([]string, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	randInt := func(min int, max int) int {
		return min + rand.Intn(max-min)
	}

	var names []string
	type Dummy struct {
		A int     `json:"a"`
		B string  `json:"b"`
		C bool    `json:"c"`
		D float64 `json:"d"`
	}
	dummyVal := Dummy{A: 1, B: "test", C: false, D: 78884543.06}
	for i := 1; i <= quantity; i++ {

		var ts TimeSeries
		ts.Name = fmt.Sprintf("http://example.com/sensor%d", i)
		ts.Meta = make(map[string]interface{})
		ts.Meta["SerialNumber"] = randInt(10000, 99999)
		ts.Meta["SerialNumberStr"] = fmt.Sprintf("sensor %d", randInt(10000, 99999))
		ts.Meta["moreInfo"] = dummyVal
		//ts.Retention = fmt.Sprintf("%d%s", randInt(1, 20), []string{"m", "h", "d", "w"}[randInt(0, 3)])
		//ts.Aggregation TODO
		ts.Type = []ValueType{Float, Bool, String}[randInt(0, 2)]

		err := client.Add(ts)
		if err != nil {
			return nil, fmt.Errorf("error adding dummy: %s", err)
		}
		names = append(names, ts.Name) // add the generated id
	}

	return names, nil
}

func TestGrpcAPI_Update(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()
	controller := *NewController(storage)
	client := setupGrpcAPI(t, controller)

	IDs, err := insertDummyData(1, client)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	ts, err := client.Get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	err = client.Update(*ts)
	if err != nil {
		t.Fatalf("Unexpected error on update: %v", err.Error())
	}

	// compare the updated and stored structs
	updatedTS, err := client.Get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	updatedBytes, _ := json.Marshal(&updatedTS)
	tsBytes, _ := json.Marshal(&ts)
	if string(updatedBytes) != string(tsBytes) {
		t.Fatalf("Mismatch updated:\n%v\n and stored:\n%v\n", string(updatedBytes), string(tsBytes))
	}
}

func TestGrpcAPI_Delete(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	client := setupGrpcAPI(t, controller)

	IDs, err := insertDummyData(1, client)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	err = client.Delete(ID)
	if err != nil {
		t.Errorf("Unexpected error on delete: %v\n", err.Error())
	}

	_, err = client.Get(ID)
	if err == nil {
		t.Error("The previous call hasn't deleted the time series!")
	}
}

func TestGrpcAPI_GetMany(t *testing.T) {

	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage, dbName, closeDB, err := setupLevelDB()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer clean(dbName)
		defer closeDB()

		controller := *NewController(storage)
		client := setupGrpcAPI(t, controller)

		insertDummyData(TOTAL, client)

		_, total, _ := client.GetMany(1, perPage)
		if total != TOTAL {
			t.Errorf("Returned total is %d instead of %d", total, TOTAL)
		}

		pages := int(math.Ceil(float64(TOTAL) / float64(perPage)))
		for page := 1; page <= pages; page++ {
			// Find out how many items should be expected on this page
			inThisPage := perPage
			if (TOTAL - (page-1)*perPage) < perPage {
				inThisPage = int(math.Mod(float64(TOTAL), float64(perPage)))
			}

			TS, _, _ := client.GetMany(page, perPage)
			if len(TS) != inThisPage {
				t.Errorf("Wrong number of entries per page. Returned %d instead of %d", len(TS), inThisPage)
			}
		}
	}
	subTest(0, 10)
	subTest(10, 10)
	subTest(55, 10)
	subTest(55, 1)
}

func TestGrpcAPI_FilterOne(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	client := setupGrpcAPI(t, controller)

	IDs, err := insertDummyData(10, client)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ID := IDs[0]

	targetTS, _ := client.Get(ID)
	matchedTS, err := client.FilterOne("name", "equals", targetTS.Name)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// check if target is returned
	targetBytes, _ := json.Marshal(&targetTS)
	matchedBytes, _ := json.Marshal(&matchedTS)
	if string(targetBytes) != string(matchedBytes) {
		t.Fatalf("Looking for:\n%v\n but matched:\n%v\n", string(targetBytes), string(matchedBytes))
	}
}

func TestGrpcAPI_Filter(t *testing.T) {
	//t.Skip("Skip until there are more meta to add")
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	client := setupGrpcAPI(t, controller)

	IDs, err := insertDummyData(10, client)
	if err != nil {
		t.Fatal(err.Error())
	}
	expected := 3

	// Modify some of them
	if len(IDs) < expected {
		t.Fatalf("Need more dummies!")
	}
	for i := 0; i < expected; i++ {
		ts, err := client.Get(IDs[i])
		if err != nil {
			t.Errorf("error getting the series:%v", err)
			return
		}
		if ts == nil {
			t.Errorf("got nil timestamp in response")
			return
		}
		ts.Meta = make(map[string]interface{})
		ts.Meta["newkey"] = "a/b"
		client.Update(*ts)
	}

	// QueryPage for format with prefix "newtype"
	_, total, err := client.Filter("meta.newkey", "prefix", "a", 1, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if total != expected {
		t.Fatalf("Returned %d matches instead of %d", total, expected)
	}
}
