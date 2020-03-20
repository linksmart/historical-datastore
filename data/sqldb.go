package data

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	_ "github.com/mattn/go-sqlite3"
)

// SqlStorage implements a SqlDB storage client for HDS Data API
type SqlStorage struct {
	pool *sql.DB
}

func NewSqlStorage(conf common.DataConf) (storage *SqlStorage, disconnect_func func() error, err error) {
	storage = new(SqlStorage)
	storage.pool, err = sql.Open("sqlite3", conf.Backend.DSN) //In future, this shall be made configurable.
	if err != nil {
		return nil, nil, err
	}
	return storage, storage.Disconnect, err
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (s *SqlStorage) Submit(data map[string]senml.Pack, streams map[string]*registry.DataStream) (err error) {
	const MAX_ENTRIES_PER_TX = 100
	tx, txErr := s.pool.Begin()
	if txErr != nil {
		return txErr
	}

	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	for dsName, pack := range data {
		valueStrings := make([]string, 0, MAX_ENTRIES_PER_TX)
		valueArgs := make([]interface{}, 0, MAX_ENTRIES_PER_TX*2)

		execStmt := func() (execErr error) {
			stmt := fmt.Sprintf("REPLACE INTO [%s] (time, value) VALUES %s",
				dsName, strings.Join(valueStrings, ","))
			_, execErr = tx.Exec(stmt, valueArgs...)
			return execErr
		}
		write := func(index int, time float64, value interface{}) (writeErr error) {
			valueStrings = append(valueStrings, "(?, ?)")
			valueArgs = append(valueArgs, time, value)
			if (index+1)%MAX_ENTRIES_PER_TX == 0 { //index+1 to ignore 0th index
				writeErr = execStmt()
				//reset the slices to empty
				valueStrings = valueStrings[:0]
				valueArgs = valueArgs[:0]
			}
			return writeErr
		}
		switch streams[dsName].Type {
		case registry.Float:
			for index, r := range pack {
				err = write(index, r.Time, *r.Value)
				if err != nil {
					return err
				}
			}
		case registry.String:
			for index, r := range pack {
				err = write(index, r.Time, r.StringValue)
				if err != nil {
					return err
				}
			}
		case registry.Bool:
			for index, r := range pack {
				err = write(index, r.Time, btoi(*r.BoolValue))
				if err != nil {
					return err
				}
			}
		case registry.Data:
			for index, r := range pack {
				err = write(index, r.Time, r.DataValue)
				if err != nil {
					return err
				}
			}
		}

		if len(valueStrings) != 0 {
			err = execStmt()
			if err != nil {
				return err
			}
		}

	}

	return nil
}

//This function converts a floating point number (which is supported by senml) to a int64
func floatTimeToInt64(senmlTime float64) int64 {
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
}

//This function converts a int64 floating point number (which is supported by senml)
func int64ToFloatTime(timeVal int64) float64 {
	return float64(timeVal) / 1e9
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

//caution: this function modifies the record and baseRecord values
func denormalizeRecord(record *senml.Record, baseRecord **senml.Record, mask DenormMask) {
	if *baseRecord == nil {
		//Store the address of the first record so that this can be used for the subsequent records.
		//Storing just the value does not work unless a deep copy is performed.
		*baseRecord = record
		if mask&FName != 0 {
			record.BaseName = record.Name
			record.Name = ""
		}
		if mask&FTime != 0 {
			record.BaseTime = record.Time
			record.Time = 0
		}
		if mask&FUnit != 0 {
			record.BaseUnit = record.Unit
			record.Unit = ""
		}
		if mask&FValue != 0 {
			record.BaseValue = record.Value
			record.Value = nil
		}
	} else {
		if mask&FName != 0 {
			record.Name = ""
		}
		if mask&FTime != 0 {
			record.Time = record.Time - (*baseRecord).BaseTime
		}
		if mask&FUnit != 0 {
			record.Unit = ""
		}
		if mask&FValue != 0 {
			if record.Value != nil && (*baseRecord).BaseValue != nil {
				*record.Value = *record.Value - *(*baseRecord).BaseValue
			}
		}
	}
}

func (s *SqlStorage) querySingleStream(q Query, stream registry.DataStream) (pack senml.Pack, total *int, err error) {
	//make a partial statement
	var unionStmt strings.Builder
	unionStmt.WriteString(fmt.Sprintf("[%s] where time BETWEEN %f and %f ",
		stream.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))

	var stmt string
	if q.count {
		total = new(int)
		stmt = "SELECT COUNT(*) FROM " + unionStmt.String()
		row := s.pool.QueryRow(stmt)
		err := row.Scan(total)
		if err != nil {
			return nil, nil, fmt.Errorf("error while querying count:%s", err)
		}
	}

	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), q.Sort, q.PerPage, (q.Page-1)*q.PerPage)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows:%s", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	var timeVal float64
	senmlName := stream.Name
	var baseRecord *senml.Record
	switch stream.Type {
	case registry.Float:
		for rows.Next() {
			var val float64
			err = rows.Scan(&timeVal, &val)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, Value: &val, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)

		}
	case registry.String:
		for rows.Next() {
			var strVal string
			err = rows.Scan(&timeVal, &strVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, StringValue: strVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	case registry.Bool:
		for rows.Next() {
			var boolVal bool
			err = rows.Scan(&timeVal, &boolVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	case registry.Data:
		for rows.Next() {
			var dataVal string
			err = rows.Scan(&timeVal, &dataVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	}
	return records, total, nil
}

func (s *SqlStorage) queryMultipleStreams(q Query, streams ...*registry.DataStream) (pack senml.Pack, total *int, err error) {
	var unionStmt strings.Builder
	unionStmt.WriteByte('(')
	unionStr := ""

	for _, stream := range streams {

		//unionStmt.WriteString(fmt.Sprintf("%s(SELECT ('%s' as 'table_name' , '%s' as 'type_val', 'time' as 'time', 'value' as 'value') FROM [%s] WHERE time BETWEEN %f and %f)", unionStr, stream.Name, stream.Type, stream.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))
		unionStmt.WriteString(fmt.Sprintf("%sSELECT  '%s' as 'table_name' , time, value FROM [%s] WHERE time BETWEEN %f and %f", unionStr, stream.Name, stream.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))
		unionStr = " UNION ALL "

	}
	unionStmt.WriteByte(')')

	//counting the number
	var stmt string
	if q.count {
		total = new(int)
		stmt = "SELECT COUNT(*) FROM " + unionStmt.String()
		row := s.pool.QueryRow(stmt)
		err := row.Scan(&total)
		if err != nil {
			return nil, nil, fmt.Errorf("error while querying count:%s", err)
		}
	}

	//query the entries
	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), q.Sort, q.PerPage, (q.Page-1)*q.PerPage)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows:%s", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	//prepare senml records
	packArr := make(map[string][]senml.Record, len(streams))

	streamMap := make(map[string]*registry.DataStream, len(streams))
	baseRecordArr := make(map[string]**senml.Record, len(streams))
	for _, stream := range streams {
		streamMap[stream.Name] = stream
		var recordPtr *senml.Record
		baseRecordArr[stream.Name] = &recordPtr
	}
	var senmlName string
	var timeVal float64
	var val interface{}
	for rows.Next() {
		err = rows.Scan(&senmlName, &timeVal, &val)
		if err != nil {
			return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
		}
		stream := *streamMap[senmlName]
		switch stream.Type {
		case registry.Float:
			floatVal := val.(float64)
			record := senml.Record{Name: senmlName, Value: &floatVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, baseRecordArr[senmlName], q.Denormalize)
			packArr[senmlName] = append(packArr[senmlName], record)
		case registry.String:
			stringVal := val.(string)
			record := senml.Record{Name: senmlName, StringValue: stringVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, baseRecordArr[senmlName], q.Denormalize)
			packArr[senmlName] = append(packArr[senmlName], record)
		case registry.Bool:
			boolVal := val.(int64) != 0
			record := senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, baseRecordArr[senmlName], q.Denormalize)
			packArr[senmlName] = append(packArr[senmlName], record)
		case registry.Data:
			dataVal := val.(string)
			record := senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: stream.Unit}
			denormalizeRecord(&record, baseRecordArr[senmlName], q.Denormalize)
			packArr[senmlName] = append(packArr[senmlName], record)
		}
	}
	for _, val := range packArr {
		records = append(records, val...)
	}
	return records, total, nil
}
func (s *SqlStorage) Query(q Query, streams ...*registry.DataStream) (pack senml.Pack, total *int, err error) {
	if len(streams) == 1 {
		return s.querySingleStream(q, *streams[0])
	} else {
		return s.queryMultipleStreams(q, streams...)
	}
}

func (s *SqlStorage) Disconnect() error {
	return s.pool.Close()
}

// CreateHandler handles the creation of a new DataStream
func (s *SqlStorage) CreateHandler(ds registry.DataStream) error {

	typeVal := map[registry.StreamType]string{
		registry.Float:  "DOUBLE",
		registry.String: "TEXT",
		registry.Bool:   "BOOLEAN",
		registry.Data:   "TEXT",
	}

	stmt := fmt.Sprintf("CREATE TABLE [%s] (time DOUBLE NOT NULL, value %s,  PRIMARY KEY (time))", ds.Name, typeVal[ds.Type])
	_, err := s.pool.Exec(stmt)
	return err
}

// UpdateHandler handles updates of a DataStream
func (s *SqlStorage) UpdateHandler(oldDS registry.DataStream, newDS registry.DataStream) error {
	//TODO supporting retention

	return nil
}

// DeleteHandler handles deletion of a DataStream
func (s *SqlStorage) DeleteHandler(ds registry.DataStream) error {
	stmt := fmt.Sprintf("DROP TABLE [%s]", ds.Name)
	_, err := s.pool.Exec(stmt)
	return err
}
