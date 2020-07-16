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

func (s *SqlStorage) Submit(data map[string]senml.Pack, series map[string]*registry.TimeSeries) (err error) {
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

func (s *SqlStorage) QueryPage(q Query, series ...*registry.TimeSeries) (pack senml.Pack, total *int, err error) {
	if len(series) == 1 {
		return s.querySingleSeries(q, *series[0], false)
	} else {
		return s.queryMultipleSeries(q, series, false)
	}
}

func (s *SqlStorage) Count(q Query, series ...*registry.TimeSeries) (total int, err error) {
	var pTotal *int
	if len(series) == 1 {
		_, pTotal, err = s.querySingleSeries(q, *series[0], true)
	} else {
		_, pTotal, err = s.queryMultipleSeries(q, series, true)
	}
	return *pTotal, err
}

func (s *SqlStorage) Delete(series []*registry.TimeSeries, from time.Time, to time.Time) (err error) {
	var stmt strings.Builder
	seperator := ""

	for _, ts := range series {
		stmt.WriteString(fmt.Sprintf("%s DELETE FROM [%s] WHERE time BETWEEN %f and %f", seperator, ts.Name, toSenmlTime(from), toSenmlTime(to)))
		seperator = ";"
	}

	_, err = s.pool.Exec(stmt.String())
	if err != nil {
		return fmt.Errorf("error executing the deletion command: %v", err)
	}

	return nil
}
func (s *SqlStorage) QueryStream(q Query, sendFunc sendFunction, series ...*registry.TimeSeries) error {
	if len(series) == 1 {
		return s.streamSingleSeries(q, sendFunc, *series[0])
	} else {
		return s.streamMultipleSeries(q, sendFunc, series)
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
	stmt := fmt.Sprintf("DROP TABLE [%s]", ts.Name)
	_, err := s.pool.Exec(stmt)
	return err
}

//This function converts a int64 floating point number (which is supported by senml)
func int64ToFloatTime(timeVal int64) float64 {
	return float64(timeVal) / 1e9
}

//This function converts a floating point number (which is supported by senml) to a int64
func floatTimeToInt64(senmlTime float64) int64 {
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
}

func toSenmlTime(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return int64ToFloatTime(t.UnixNano())
}

func fromSenmlTime(t float64) time.Time {
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

func (s *SqlStorage) querySingleSeries(q Query, stream registry.TimeSeries, countOnly bool) (pack senml.Pack, total *int, err error) {
	//make a partial statement
	var unionStmt strings.Builder
	unionStmt.WriteString(fmt.Sprintf("[%s] where time BETWEEN %f and %f ",
		stream.Name, toSenmlTime(q.From), toSenmlTime(q.To)))

	var stmt string
	if q.Count || countOnly {
		total = new(int)
		stmt = "SELECT COUNT(*) FROM " + unionStmt.String()
		row := s.pool.QueryRow(stmt)
		err := row.Scan(total)
		if err != nil {
			return nil, nil, fmt.Errorf("error while querying Count:%s", err)
		}
		if countOnly {
			return nil, total, nil
		}
	}
	order := common.Desc
	if q.SortAsc {
		order = common.Asc
	}
	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), order, q.PerPage, (q.Page-1)*q.PerPage)
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

func (s *SqlStorage) queryMultipleSeries(q Query, series []*registry.TimeSeries, countOnly bool) (pack senml.Pack, total *int, err error) {
	var unionStmt strings.Builder
	unionStmt.WriteByte('(')
	unionStr := ""

	for _, ts := range series {

		//unionStmt.WriteString(fmt.Sprintf("%s(SELECT ('%s' as 'table_name' , '%s' as 'type_val', 'time' as 'time', 'value' as 'value') FROM [%s] WHERE time BETWEEN %f and %f)", unionStr, ts.Name, ts.Type, ts.Name, toSenmlTime(q.From), toSenmlTime(q.To)))
		unionStmt.WriteString(fmt.Sprintf("%sSELECT  '%s' as 'table_name' , time, value FROM [%s] WHERE time BETWEEN %f and %f", unionStr, ts.Name, ts.Name, toSenmlTime(q.From), toSenmlTime(q.To)))
		unionStr = " UNION ALL "

	}
	unionStmt.WriteByte(')')

	//counting the number
	var stmt string
	if q.Count || countOnly {
		total = new(int)
		stmt = "SELECT COUNT(*) FROM " + unionStmt.String()
		row := s.pool.QueryRow(stmt)
		err := row.Scan(total)
		if err != nil {
			return nil, nil, fmt.Errorf("error while querying Count:%s", err)
		}
		if countOnly {
			return nil, total, nil
		}
	}

	//query the entries
	order := common.Desc
	if q.SortAsc {
		order = common.Asc
	}
	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), order, q.PerPage, (q.Page-1)*q.PerPage)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows:%s", err)
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
			return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
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

func (s *SqlStorage) streamSingleSeries(q Query, sendFunc sendFunction, series registry.TimeSeries) error {
	//make a partial statement
	var unionStmt strings.Builder
	fromTime := toSenmlTime(q.From)
	toTime := toSenmlTime(q.To)
	unionStmt.WriteString(fmt.Sprintf("[%s] where time BETWEEN %f and %f ",
		series.Name, fromTime, toTime))

	order := common.Desc
	if q.SortAsc {
		order = common.Asc
	}
	limitStr := ""
	if q.Limit != 0 {
		limitStr = fmt.Sprintf("LIMIT %d OFFSET %d", q.Limit, q.Offset)
	}

	intervalSec := q.Interval.Seconds()
	var stmt string
	if q.Aggregator != "" {
		if series.Type == registry.Float {
			stmt = fmt.Sprintf(`WITH RECURSIVE %s 
                                   SELECT s AS time ,%s(value) AS value 
                                   FROM  (SELECT * FROM %s) JOIN INTERVAL on time between s and e GROUP BY e ORDER BY time %s %s`,
				aggrRangeQuery(fromTime, toTime, intervalSec),
				q.Aggregator, unionStmt.String(), order, limitStr)
		} else {
			return fmt.Errorf("aggregation is not allowed on non numeric series %s", series.Name)
		}
	} else {
		stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s %s", unionStmt.String(), order, limitStr)
	}
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return fmt.Errorf("error while querying rows:%s", err)
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
			err = rows.Scan(&timeVal, &val)
			if err != nil {
				return fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, Value: &val, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				sendFunc(records)
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.String:
		for rows.Next() {
			var strVal string
			err = rows.Scan(&timeVal, &strVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, StringValue: strVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				sendFunc(records)
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.Bool:
		for rows.Next() {
			var boolVal bool
			err = rows.Scan(&timeVal, &boolVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				sendFunc(records)
				records = records[:0]
				baseRecord = nil
			}
		}
	case registry.Data:
		for rows.Next() {
			var dataVal string
			err = rows.Scan(&timeVal, &dataVal)
			if err != nil {
				return fmt.Errorf("error while scanning query results:%s", err)
			}
			record := senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal, Unit: series.Unit}
			denormalizeRecord(&record, &baseRecord, q.Denormalize)
			records = append(records, record)
			recordCount++
			if recordCount == q.PerPage {
				//prepare for the next round by resetting the slice
				recordCount = 0
				sendFunc(records)
				records = records[:0]
				baseRecord = nil
			}
		}
	}
	if recordCount != 0 { //send the last page
		sendFunc(records)
	}
	return nil
}

func aggrFunc(aggregator string) interface{} {

}

func (s *SqlStorage) streamMultipleSeries(q Query, sendFunc sendFunction, series []*registry.TimeSeries) error {
	var unionStmt strings.Builder
	unionStmt.WriteByte('(')
	unionStr := ""

	for _, ts := range series {

		//unionStmt.WriteString(fmt.Sprintf("%s(SELECT ('%s' as 'table_name' , '%s' as 'type_val', 'time' as 'time', 'value' as 'value') FROM [%s] WHERE time BETWEEN %f and %f)", unionStr, ts.Name, ts.Type, ts.Name, toSenmlTime(q.From), toSenmlTime(q.To)))
		unionStmt.WriteString(fmt.Sprintf("%sSELECT  '%s' as 'table_name' , time, value FROM [%s] WHERE time BETWEEN %f and %f", unionStr, ts.Name, ts.Name, toSenmlTime(q.From), toSenmlTime(q.To)))
		unionStr = " UNION ALL "

	}
	unionStmt.WriteByte(')')

	//query the entries
	order := common.Desc
	if q.SortAsc {
		order = common.Asc
	}
	limitStr := ""
	if q.Limit != 0 {
		limitStr = fmt.Sprintf("LIMIT %d OFFSET %d", q.Limit, q.Offset)
	}
	stmt := fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s %s", unionStmt.String(), order, limitStr)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return fmt.Errorf("error while querying rows:%s", err)
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
			return fmt.Errorf("error while scanning query results:%s", err)
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
			sendFunc(records)
			records = records[:0]
			baseRecord = nil
		}

	}
	if recordCount != 0 { //send the last page
		sendFunc(records)
	}
	return nil
}

// Gets the recursive query making the table containing ranges
func aggrRangeQuery(from float64, to float64, durSec float64) string {
	retStr := fmt.Sprintf("Intervals(s,e) AS ("+
		"SELECT %f AS s, %f AS e"+
		"UNION ALL"+
		"SELECT s-%f AS s, e-%f AS e FROM Intervals"+
		"WHERE s > %f"+
		") ", to-durSec, to, durSec, durSec, from)
	return retStr
}
