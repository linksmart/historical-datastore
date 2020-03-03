package datastore

import (
	"encoding/json"
	"fmt"
	"time"

	tsdb "github.com/dschowta/lite.tsdb"
	"github.com/farshidtz/senml"
)

const (
	ASC  = "asc"
	DESC = "desc"
)

type SenmlDataStore struct {
	tsdb tsdb.TSDB
}

type DenormMask int

const (
	FName DenormMask = 1 << iota
	FTime
	FUnit
	FValue
	FSum
)

type Query struct {
	//A comma separated senml names
	Series string

	From float64
	To   float64
	//Sorting order:
	//Possible values are ASC and DESC
	//ASC : The time Series will have the oldest data first
	//DESC: The time Series will have the latest  data first.
	Sort string

	//Number of entries to be returned per request. This is used for pagination. The next sequence is found out using NextEntry function
	MaxEntries int

	//comma separated fields for denormalization (see https://tools.ietf.org/html/rfc8428#section-4.6). Currently only "name" and "time" are supported.
	Denormalize DenormMask
}

type SenMLDBRecord struct {
	Unit        string   `json:"u,omitempty" `
	UpdateTime  float64  `json:"ut,omitempty"`
	Value       *float64 `json:"v,omitempty" `
	StringValue string   `json:"vs,omitempty" `
	DataValue   string   `json:"vd,omitempty"  `
	BoolValue   *bool    `json:"vb,omitempty" `

	Sum *float64 `json:"s,omitempty" `
}

func (bdb *SenmlDataStore) Connect(path string) error {
	config := tsdb.BoltDBConfig{Path: path}
	var err error
	bdb.tsdb, err = tsdb.Open(config)
	return err
}

func (bdb SenmlDataStore) Disconnect() error {
	return bdb.tsdb.Close()
}
func NewBoltSenMLRecord(record senml.Record) SenMLDBRecord {
	return SenMLDBRecord{
		record.Unit,
		record.UpdateTime,
		record.Value,
		record.StringValue,
		record.DataValue,
		record.BoolValue,
		record.Sum,
	}
}

func ToSenmlTime(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return int64ToFloatTime(t.UnixNano())
}

func FromSenmlTime(t float64) time.Time {
	return time.Unix(0, floatTimeToInt64(t))
}

//This function converts a floating point number (which is supported by senml) to a bytearray
func floatTimeToInt64(senmlTime float64) int64 {
	//sec, frac := math.Modf(senmlTime)
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
}

//This function converts a bytearray floating point number (which is supported by senml)
func int64ToFloatTime(timeVal int64) float64 {
	return float64(timeVal) / 1e9
}

//Create a new bucket
func (bdb SenmlDataStore) Create(name string) error {
	return bdb.tsdb.Create(name)
}

func (bdb SenmlDataStore) Add(senmlPack senml.Pack) error {

	// Fill the data map with provided data points
	pack := senmlPack.Normalize()

	seriesMap := make(map[string][]tsdb.TimeEntry)
	for _, r := range pack {
		if "" != r.Name {
			b, err := json.Marshal(NewBoltSenMLRecord(r))
			if err != nil {
				return err
			}
			entry := tsdb.TimeEntry{floatTimeToInt64(r.Time), b}

			seriesMap[r.Name] = append(seriesMap[r.Name], entry)
		} else {
			return fmt.Errorf("Senml record with Empty name")
		}

	}

	for name, series := range seriesMap {
		err := bdb.tsdb.Add(name, series)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bdb SenmlDataStore) Get(series string) (senml.Pack, error) {
	var senmlPack senml.Pack
	timeSeriesCh, errCh := bdb.tsdb.GetOnChannel(series)

	senmlPack = iterateOverChannel(timeSeriesCh, 0, series)
	//Check the error channel
	err := <-errCh

	return senmlPack, err
}

//Query the data store for a particular range. This gives the response in multiple pages
func (bdb SenmlDataStore) Query(query Query) (senml.Pack, *float64, error) {
	var senmlPack senml.Pack

	tsQuery := tsdb.Query{
		MaxEntries: query.MaxEntries,
		Series:     query.Series,
		Sort:       query.Sort,
		To:         floatTimeToInt64(query.To),
		From:       floatTimeToInt64(query.From),
	}

	timeSeriesCh, nextEntryCh, errCh := bdb.tsdb.QueryOnChannel(tsQuery)

	senmlPack = iterateOverChannel(timeSeriesCh, query.Denormalize, query.Series)
	//Check the error channel
	nextEntry := <-nextEntryCh
	err := <-errCh

	if nextEntry != nil {
		nextEntryf := int64ToFloatTime(*nextEntry)
		return senmlPack, &nextEntryf, err
	} else {
		return senmlPack, nil, err
	}
}

func iterateOverChannel(timeSeriesCh <-chan tsdb.TimeEntry, denormalize DenormMask, seriesName string) (senmlPack senml.Pack) {

	var baseTime float64

	for timeEntry := range timeSeriesCh {
		var timeRecord SenMLDBRecord
		var curBaseName, curName string
		var curTime float64
		var curBaseTime float64

		err := json.Unmarshal(timeEntry.Value, &timeRecord)
		if err != nil {
			fmt.Printf("Error while unmarshalling %s", err)
			continue
		}

		if denormalize&FName != 0 {
			if senmlPack == nil { //First time
				curBaseName = seriesName
			} else {
				curBaseName = ""
			}
			curName = ""
		} else {
			curName = seriesName

		}

		if denormalize&FTime != 0 {
			if senmlPack == nil { //First time
				baseTime = int64ToFloatTime(timeEntry.Time)
				curBaseTime = baseTime
				curTime = 0
			} else {
				curBaseTime = 0
				curTime = int64ToFloatTime(timeEntry.Time) - baseTime
			}
		} else {
			curBaseTime = 0
			curTime = int64ToFloatTime(timeEntry.Time)
		}
		record := senml.Record{
			BaseName:    curBaseName,
			Name:        curName,
			Unit:        timeRecord.Unit,
			BaseTime:    curBaseTime,
			Time:        curTime,
			UpdateTime:  timeRecord.UpdateTime,
			Value:       timeRecord.Value,
			StringValue: timeRecord.StringValue,
			DataValue:   timeRecord.DataValue,
			BoolValue:   timeRecord.BoolValue,
			Sum:         timeRecord.Sum,
		}
		senmlPack = append(senmlPack, record)
	}
	return senmlPack
}

func (bdb SenmlDataStore) GetPages(query Query) ([]float64, int, error) {
	tsQuery := tsdb.Query{
		MaxEntries: query.MaxEntries,
		Series:     query.Series,
		Sort:       query.Sort,
		To:         floatTimeToInt64(query.To),
		From:       floatTimeToInt64(query.From),
	}
	pages, count, err := bdb.tsdb.GetPages(tsQuery)

	if err != nil {
		return nil, 0, err
	}
	fpages := make([]float64, 0, len(pages))
	for _, page := range pages {
		fpages = append(fpages, int64ToFloatTime(page))
	}

	return fpages, count, nil
}

func (bdb *SenmlDataStore) Delete(series string) error {
	err := bdb.tsdb.Delete(series)
	if err == tsdb.ErrSeriesNotFound {
		err = ErrSeriesNotFound
	}
	return err
}
