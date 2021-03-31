package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/linksmart/historical-datastore/common"
	"github.com/linksmart/historical-datastore/registry"
	_ "github.com/mattn/go-sqlite3"
)

var sqlQueryTimeout = 30 * time.Second

// SqlStorage implements a SqlDB storage client for HDS Data API
type SqlStorage struct {
	pool        *sql.DB
	updateMutex sync.Mutex
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

func (s *SqlStorage) submissionThread() {

}
func (s *SqlStorage) Submit(ctx context.Context, data map[string]senml.Pack, series map[string]*registry.TimeSeries) (err error) {
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	tx, txErr := s.pool.BeginTx(ctx, nil)
	if txErr != nil {
		return txErr
	}

	err = s.submit(tx, ctx, data, series)

	if err != nil {
		rollbackErr := tx.Rollback()
		errStr := fmt.Sprintf("error inserting: %s", err)
		if rollbackErr != nil {
			errStr += fmt.Sprintf(", error during rollback: %s", rollbackErr)
		}
		return fmt.Errorf(errStr)

	}

	return tx.Commit()

}

func (s *SqlStorage) submit(tx *sql.Tx, ctx context.Context, data map[string]senml.Pack, series map[string]*registry.TimeSeries) (err error) {
	const MAX_ENTRIES_PER_TX = 100
	for dsName, pack := range data {
		valueStrings := make([]string, 0, MAX_ENTRIES_PER_TX)
		valueArgs := make([]interface{}, 0, MAX_ENTRIES_PER_TX*2)

		execStmt := func() (execErr error) {
			stmt := fmt.Sprintf("REPLACE INTO [%s] (time, value) VALUES %s",
				dsName, strings.Join(valueStrings, ","))
			_, execErr = tx.ExecContext(ctx, stmt, valueArgs...)
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
		switch series[dsName].Type {
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
func (s *SqlStorage) QueryPage(ctx context.Context, q Query, series ...*registry.TimeSeries) (pack senml.Pack, total *int, err error) {
	if len(series) == 1 {
		return s.querySingleSeries(ctx, q, *series[0])
	} else {
		return s.queryMultipleSeries(ctx, q, series)
	}
}

func (s *SqlStorage) Count(ctx context.Context, q Query, series ...*registry.TimeSeries) (int, error) {
	total := new(int)
	stmt, err := makeQuery(q, true, false, series...)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, sqlQueryTimeout)
	defer cancel()
	row := s.pool.QueryRowContext(ctx, stmt)

	err = row.Scan(total)
	if err != nil {
		return 0, fmt.Errorf("error while querying Count: %w", err)
	}
	return *total, err
}

func (s *SqlStorage) Delete(ctx context.Context, series []*registry.TimeSeries, from time.Time, to time.Time) (err error) {
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	var stmt strings.Builder
	seperator := ""

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

	for _, ts := range series {
		stmt.WriteString(fmt.Sprintf("%s DELETE FROM [%s] WHERE time BETWEEN %f and %f", seperator, ts.Name, ToSenmlTime(from), ToSenmlTime(to)))
		seperator = ";"
	}

	_, err = tx.ExecContext(ctx, stmt.String())
	if err != nil {
		return err
	}

	return nil
}
func (s *SqlStorage) QueryStream(ctx context.Context, q Query, sendFunc sendFunction, series ...*registry.TimeSeries) error {
	if len(series) == 1 {
		return s.streamSingleSeries(ctx, q, sendFunc, *series[0])
	} else {
		return s.streamMultipleSeries(ctx, q, sendFunc, series)
	}
}

func (s *SqlStorage) Disconnect() error {
	return s.pool.Close()
}

// CreateHandler handles the creation of a new TimeSeries
func (s *SqlStorage) CreateHandler(ts registry.TimeSeries) error {

	typeVal := map[registry.ValueType]string{
		registry.Float:  "DOUBLE",
		registry.String: "TEXT",
		registry.Bool:   "BOOLEAN",
		registry.Data:   "TEXT",
	}

	stmt := fmt.Sprintf("CREATE TABLE [%s] (time DOUBLE NOT NULL, value %s,  PRIMARY KEY (time))", ts.Name, typeVal[ts.Type])
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	_, err := s.pool.Exec(stmt)
	return err
}

// UpdateHandler handles updates of a TimeSeries
func (s *SqlStorage) UpdateHandler(oldDS registry.TimeSeries, newDS registry.TimeSeries) error {
	//TODO supporting retention

	return nil
}

// DeleteHandler handles deletion of a TimeSeries
func (s *SqlStorage) DeleteHandler(ts registry.TimeSeries) error {
	tableExists, err := s.TableExists(ts)
	if err != nil {
		return err
	}
	if tableExists == false {
		return nil
	}
	s.updateMutex.Lock()
	defer s.updateMutex.Unlock()
	stmt := fmt.Sprintf("DROP TABLE [%s]", ts.Name)
	_, err = s.pool.Exec(stmt)
	return err
}

func (s *SqlStorage) TableExists(ts registry.TimeSeries) (bool, error) {
	var total int
	stmt := fmt.Sprintf("SELECT  COUNT(*) FROM sqlite_master WHERE type='table' AND name='%s'", ts.Name)

	row := s.pool.QueryRow(stmt)

	err := row.Scan(&total)
	if err != nil {
		return false, fmt.Errorf("error while checking if table exists or not: %w", err)
	}
	return total != 0, nil

}

//This function converts a int64 floating point number (which is supported by senml)
func int64ToFloatTime(timeVal int64) float64 {
	return float64(timeVal) / 1e9
}

//This function converts a floating point number (which is supported by senml) to a int64
func floatTimeToInt64(senmlTime float64) int64 {
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
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
		if mask&DenormMaskName != 0 {
			record.BaseName = record.Name
			record.Name = ""
		}
		if mask&DenormMaskTime != 0 {
			record.BaseTime = record.Time
			record.Time = 0
		}
		if mask&DenormMaskUnit != 0 {
			record.BaseUnit = record.Unit
			record.Unit = ""
		}
		if mask&DenormMaskValue != 0 {
			record.BaseValue = record.Value
			record.Value = nil
		}
	} else {
		if mask&DenormMaskName != 0 {
			record.Name = ""
		}
		if mask&DenormMaskTime != 0 {
			record.Time = record.Time - (*baseRecord).BaseTime
		}
		if mask&DenormMaskUnit != 0 {
			record.Unit = ""
		}
		if mask&DenormMaskValue != 0 {
			if record.Value != nil && (*baseRecord).BaseValue != nil {
				*record.Value = *record.Value - *(*baseRecord).BaseValue
			}
		}
	}
}

func (s *SqlStorage) querySingleSeries(ctx context.Context, q Query, series registry.TimeSeries) (pack senml.Pack, total *int, err error) {
	if q.Count {
		total = new(int)
		*total, err = s.Count(ctx, q, &series)
		if err != nil {
			return nil, nil, err
		}
	}
	stmt, err := makeQuery(q, false, false, &series)
	if err != nil {
		return nil, nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, sqlQueryTimeout)
	defer cancel()
	rows, err := s.pool.QueryContext(ctx, stmt)

	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows: %w", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	var timeVal float64
	senmlName := series.Name
	var baseRecord *senml.Record
	switch series.Type {
	case registry.Float:
		for rows.Next() {
			var val float64
			err = rows.Scan(&senmlName, &timeVal, &val)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, Value: &val, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)

		}
	case registry.String:
		for rows.Next() {
			var strVal string
			err = rows.Scan(&senmlName, &timeVal, &strVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, StringValue: strVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	case registry.Bool:
		for rows.Next() {
			var boolVal bool
			err = rows.Scan(&senmlName, &timeVal, &boolVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	case registry.Data:
		for rows.Next() {
			var dataVal string
			err = rows.Scan(&senmlName, &timeVal, &dataVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
		}
	}
	return records, total, nil
}

func (s *SqlStorage) queryMultipleSeries(ctx context.Context, q Query, series []*registry.TimeSeries) (pack senml.Pack, total *int, err error) {
	if q.Count {
		total = new(int)
		*total, err = s.Count(ctx, q, series...)
		if err != nil {
			return nil, nil, err
		}
	}
	var stmt string

	stmt, err = makeQuery(q, false, false, series...)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, sqlQueryTimeout)
	defer cancel()
	rows, err := s.pool.QueryContext(ctx, stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows: %w", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	seriesMap := make(map[string]*registry.TimeSeries, len(series))

	for _, ts := range series {
		seriesMap[ts.Name] = ts
	}
	var senmlName string
	var timeVal float64
	var val interface{}

	denormMask := q.Denormalize &^ DenormMaskName // Reset the DenormMaskName. denormalizing the name is not supported in case of multiseries requests
	var baseRecord *senml.Record

	for rows.Next() {
		err = rows.Scan(&senmlName, &timeVal, &val)
		if err != nil {
			return nil, nil, fmt.Errorf("error while scanning query results: %s", err)
		}
		series := *seriesMap[senmlName]

		var record senml.Record

		switch series.Type {
		case registry.Float:
			floatVal, ok := val.(float64)
			if !ok {
				return nil, nil, fmt.Errorf("error while scanning float64 query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, Value: &floatVal, Time: timeVal, Unit: series.Unit}

		case registry.String:
			stringVal, ok := val.(string)
			if !ok {
				return nil, nil, fmt.Errorf("error while scanning string query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, StringValue: stringVal, Time: timeVal, Unit: series.Unit}
		case registry.Bool:
			var boolVal bool
			switch retType := val.(type) { //Some of the OS environments return the type as boolean even if the expected type is int64. This issue was found in Travis CI.
			case int64:
				boolVal = val.(int64) != 0
			case bool:
				boolVal = val.(bool)
			default:
				return nil, nil, fmt.Errorf("error while scanning boolean query result: unexpected type %v obtained", retType)
			}
			record = senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: series.Unit}
		case registry.Data:
			dataVal, ok := val.(string)
			if !ok {
				return nil, nil, fmt.Errorf("error while scanning boolean query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: series.Unit}
		}

		denormalizeRecord(&record, &baseRecord, denormMask)
		records = append(records, record)

	}

	return records, total, nil
}

func (s *SqlStorage) streamSingleSeries(ctx context.Context, q Query, sendFunc sendFunction, series registry.TimeSeries) (err error) {
	stmt, err := makeQuery(q, false, true, &series)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, sqlQueryTimeout)
	defer cancel()
	rows, err := s.pool.QueryContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("error while querying rows: %w", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	var timeVal float64
	senmlName := series.Name
	var baseRecord *senml.Record
	recordCount := 0
	switch series.Type {
	case registry.Float:
		for rows.Next() {
			var val float64
			err = rows.Scan(&senmlName, &timeVal, &val)
			if err != nil {
				return fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, Value: &val, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				if err = sendFunc(records); err != nil {
					return err
				}
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.String:
		for rows.Next() {
			var strVal string
			err = rows.Scan(&senmlName, &timeVal, &strVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, StringValue: strVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				if err = sendFunc(records); err != nil {
					return err
				}
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.Bool:
		for rows.Next() {
			var boolVal bool
			err = rows.Scan(&senmlName, &timeVal, &boolVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				if err = sendFunc(records); err != nil {
					return err
				}
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.Data:
		for rows.Next() {
			var dataVal string
			err = rows.Scan(&senmlName, &timeVal, &dataVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results: %s", err)
			}
			record := senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				if err = sendFunc(records); err != nil {
					return err
				}
				records = records[:0]
				baseRecord = nil
			}
		}
	}
	if recordCount != 0 { //send the last page
		if err = sendFunc(records); err != nil {
			return err
		}
	}
	return nil
}

func (s *SqlStorage) streamMultipleSeries(ctx context.Context, q Query, sendFunc sendFunction, series []*registry.TimeSeries) error {

	stmt, err := makeQuery(q, false, true, series...)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, sqlQueryTimeout)
	defer cancel()
	rows, err := s.pool.QueryContext(ctx, stmt)

	if err != nil {
		return fmt.Errorf("error while querying rows: %w", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	seriesMap := make(map[string]*registry.TimeSeries, len(series))

	for _, ts := range series {
		seriesMap[ts.Name] = ts
	}
	var senmlName string
	var timeVal float64
	var val interface{}
	denormMask := q.Denormalize &^ DenormMaskName // Reset the DenormMaskName. denormalizing the name is not supported  in case of multiseries requests
	var baseRecord *senml.Record
	recordCount := 0
	for rows.Next() {
		err = rows.Scan(&senmlName, &timeVal, &val)
		if err != nil {
			return fmt.Errorf("error while scanning query results: %s", err)
		}
		series := *seriesMap[senmlName]
		var record senml.Record
		switch series.Type {
		case registry.Float:
			floatVal, ok := val.(float64)
			if !ok {
				return fmt.Errorf("error while scanning float64 query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, Value: &floatVal, Time: timeVal, Unit: series.Unit}

		case registry.String:
			stringVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("error while scanning string query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, StringValue: stringVal, Time: timeVal, Unit: series.Unit}

		case registry.Bool:
			var boolVal bool
			switch retType := val.(type) { //Some of the OS environments return the type as boolean even if the expected type is int64. This issue was found in Travis CI.
			case int64:
				boolVal = val.(int64) != 0
			case bool:
				boolVal = val.(bool)
			default:
				return fmt.Errorf("error while scanning boolean query result: unexpected type %v obtained", retType)
			}
			record = senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: series.Unit}

		case registry.Data:
			dataVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("error while scanning boolean query result: unexpected type obtained")
			}
			record = senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: series.Unit}
		}
		denormalizeRecord(&record, &baseRecord, denormMask)
		records = append(records, record)
		recordCount++
		if recordCount == q.PerPage {
			//prepare for the next round by resetting the slice
			recordCount = 0
			if err = sendFunc(records); err != nil {
				return err
			}
			records = records[:0]
			baseRecord = nil
		}

	}
	if recordCount != 0 { //send the last page
		if err = sendFunc(records); err != nil {
			return err
		}
	}
	return nil
}

// Gets the recursive query making the table containing ranges
func makeQuery(q Query, count bool, stream bool, series ...*registry.TimeSeries) (stmt string, err error) {
	fromTime := ToSenmlTime(q.From)
	toTime := ToSenmlTime(q.To)
	//query the entries
	order := common.Desc
	if q.SortAsc {
		order = common.Asc
	}

	// set limit string
	limitStr := ""
	if stream && q.Limit != 0 { //for stream queries, limit with the provided limit
		limitStr = fmt.Sprintf("LIMIT %d OFFSET %d", q.Limit, q.Offset)
	}
	if !stream && !count { //for page queries, limit with pagination parameters. Count does not have the limit
		limitStr = fmt.Sprintf("LIMIT %d OFFSET %d", q.PerPage, (q.Page-1)*q.PerPage)
	}

	if q.AggrFunc != "" {
		durSec := q.AggrWindow.Seconds()
		// create union of multiple series
		timeAggr := fmt.Sprintf("%f- MAX(ROUND(((%f-time)/%f)-0.5),0)*%f", toTime, toTime, durSec, durSec)
		var tableUnion strings.Builder
		unionStr := ""
		for _, ts := range series {
			if ts.Type != registry.Float {
				return "", fmt.Errorf("aggregation is not supported for non-numeric series %s", ts.Name)
			}
			tableUnion.WriteString(fmt.Sprintf(`%sSELECT  '%s' AS 'table_name' , %s AS time, value 
														FROM [%s] 
														WHERE time BETWEEN %f AND %f`,
				unionStr, ts.Name, timeAggr, ts.Name, fromTime, toTime))
			unionStr = " UNION ALL "
		}
		stmt = fmt.Sprintf(`WITH raw_data(table_name,time,value) AS (
										%s
                                    )`, tableUnion.String())
		if count {
			stmt = stmt +
				fmt.Sprintf(`
						SELECT  COUNT(*) FROM (SELECT DISTINCT time,table_name
						FROM raw_data  %s)`, limitStr)
		} else {
			stmt = stmt +
				fmt.Sprintf(`
						SELECT  table_name, time ,%s(value)*1.0 AS value
						FROM raw_data GROUP BY time,table_name ORDER BY time %s %s`, aggrToSqlFunc(q.AggrFunc), order, limitStr)
		}
	} else {

		// create union of multiple series
		var tableUnion strings.Builder
		unionStr := ""
		for _, ts := range series {
			tableUnion.WriteString(fmt.Sprintf("%sSELECT  '%s' as 'table_name' , time, value FROM [%s] WHERE time BETWEEN %f AND %f", unionStr, ts.Name, ts.Name, fromTime, toTime))
			unionStr = " UNION ALL "
		}

		if count == true {
			stmt = fmt.Sprintf("SELECT COUNT(*) FROM (%s) %s", tableUnion.String(), limitStr)
		} else {
			stmt = fmt.Sprintf("SELECT * FROM (%s) ORDER BY time %s %s", tableUnion.String(), order, limitStr)
		}
	}
	return stmt, nil
}

func aggrToSqlFunc(aggrName string) (sqlFunc string) {
	const (
		AGGR_AVG   = "AVG"
		AGGR_SUM   = "SUM"
		AGGR_MIN   = "MIN"
		AGGR_MAX   = "MAX"
		AGGR_COUNT = "COUNT"
	)
	switch aggrName {
	case "mean":
		return AGGR_AVG
	case "sum":
		return AGGR_SUM
	case "min":
		return AGGR_MIN
	case "max":
		return AGGR_MAX
	case "count":
		return AGGR_COUNT
	default:
		panic("Invalid aggregation:" + aggrName)
	}
}
