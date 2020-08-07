// Copyright 2016 Fraunhofer Institute for Applied Information Technology FIT

package registry

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/linksmart/historical-datastore/common"
)

func setupLevelDB() (Storage, string, func() error, error) {
	// Temp database file
	// Replace Windows-based backslashes with slash (not parsed as Path by net/url)
	os_temp := strings.Replace(os.TempDir(), "\\", "/", -1)
	dbName := fmt.Sprintf("%d.ldb", time.Now().UnixNano())
	temp_file := fmt.Sprintf("%s/hds-test/%s", os_temp, dbName)
	conf := common.RegConf{
		Backend: common.RegBackendConf{
			DSN: temp_file,
		},
	}
	storage, closeDB, err := NewLevelDBStorage(conf, nil)
	if err != nil {
		return nil, dbName, nil, err
	}

	return storage, dbName, closeDB, nil
}

// Remove temporary database files
func clean(dbName string) {
	temp_dir := fmt.Sprintf("%s/hds-test/%s", os.TempDir(), dbName)
	err := os.RemoveAll(temp_dir)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestLevelDBAdd(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	var ts TimeSeries
	ts.Name = "any_url"
	//ts.Retention = ""
	//ts.Aggregation TODO
	ts.Type = String

	addedTS, err := storage.add(ts)
	if err != nil {
		t.Fatalf("Received unexpected error on add: %v", err.Error())
	}

	getTS, err := storage.get(addedTS.Name)
	if err != nil {
		t.Fatalf("Received unexpected error on get: %v", err.Error())
	}

	// compare added and retrieved data
	addedBytes, _ := json.Marshal(&addedTS)
	getBytes, _ := json.Marshal(&getTS)
	if string(getBytes) != string(addedBytes) {
		t.Fatalf("Mismatch:\n added:\n%v\n retrieved:\n%v\n", string(addedBytes), string(getBytes))
	}
}

func TestLevelDBGet(t *testing.T) {
	t.Skip("Tested in TestLevelDBAdd")
}

func TestLevelDBUpdate(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()
	controller := *NewController(storage)
	IDs, err := generateDummyData(1, controller)
	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	ts, err := storage.get(ID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err.Error())
	}

	updatedTS, err := storage.update(ID, *ts)
	if err != nil {
		t.Fatalf("Unexpected error on update: %v", err.Error())
	}

	// compare the updated and stored structs
	updatedBytes, _ := json.Marshal(&updatedTS)
	tsBytes, _ := json.Marshal(&ts)
	if string(updatedBytes) != string(tsBytes) {
		t.Fatalf("Mismatch updated:\n%v\n and stored:\n%v\n", string(updatedBytes), string(tsBytes))
	}
}

func TestLevelDBDelete(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	IDs, err := generateDummyData(1, controller)

	if err != nil {
		t.Fatal(err.Error())
	}
	ID := IDs[0]

	err = storage.delete(ID)
	if err != nil {
		t.Errorf("Unexpected error on delete: %v\n", err.Error())
	}

	_, err = storage.get(ID)
	if err == nil {
		t.Error("The previous call hasn't deleted the time series!")
	}
}

func TestLevelDBGetMany(t *testing.T) {

	// Check based on different inputs
	subTest := func(TOTAL int, perPage int) {
		storage, dbName, closeDB, err := setupLevelDB()
		if err != nil {
			t.Fatal(err.Error())
		}
		defer clean(dbName)
		defer closeDB()

		controller := *NewController(storage)
		generateDummyData(TOTAL, controller)

		_, total, _ := storage.getMany(1, perPage)
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

			TS, _, _ := storage.getMany(page, perPage)
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

func TestLevelDBGetCount(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	// Get the current total
	c1, err := storage.getTotal()
	if err != nil {
		t.Fatal(err.Error())
	}

	// Add few time series

	const total = 5
	generateDummyData(total, controller)

	c2, err := storage.getTotal()
	if err != nil {
		t.Fatal(err.Error())
	}
	if total != c2-c1 {
		t.Fatalf("Created %d but counted %d time series!", total, c2-c1)
	}
}

func TestLevelDBPathFilterOne(t *testing.T) {
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()
	controller := *NewController(storage)
	IDs, err := generateDummyData(10, controller)
	if err != nil {
		t.Fatalf(err.Error())
	}
	ID := IDs[0]

	targetTS, _ := storage.get(ID)
	matchedTS, err := storage.filterOne("name", "equals", targetTS.Name)
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

func TestLevelDBPathFilter(t *testing.T) {
	//t.Skip("Skip until there are more meta to add")
	storage, dbName, closeDB, err := setupLevelDB()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer clean(dbName)
	defer closeDB()

	controller := *NewController(storage)
	IDs, err := generateDummyData(10, controller)

	if err != nil {
		t.Fatal(err.Error())
	}
	expected := 3

	// Modify some of them
	if len(IDs) < expected {
		t.Fatalf("Need more dummies!")
	}
	for i := 0; i < expected; i++ {
		ts, _ := storage.get(IDs[i])
		ts.Meta["newkey"] = "a/b"
		storage.update(ts.Name, *ts)
	}

	// QueryPage for format with prefix "newtype"
	_, total, err := storage.filter("meta.newkey", "prefix", "a", 1, 100)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if total != expected {
		t.Fatalf("Returned %d matches instead of %d", total, expected)
	}
}
