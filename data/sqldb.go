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

func (s *SqlStorage) Submit(data map[string]senml.Pack, sources map[string]*registry.DataStream) error {
	for ds, pack := range data {
		valueStrings := make([]string, 0, len(pack))
		valueArgs := make([]interface{}, 0, len(pack)*2)
		switch sources[ds].Type {
		case registry.Float:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, *r.Value)
			}
		case registry.String:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, r.StringValue)
			}
		case registry.Bool:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, btoi(*r.BoolValue))
			}
		case registry.Data:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, r.DataValue)
			}
		}

		stmt := fmt.Sprintf("INSERT INTO [%s] (time, value) VALUES %s",
			ds, strings.Join(valueStrings, ","))
		_, err := s.pool.Exec(stmt, valueArgs...)
		return err
	}

	return nil
}

//This function converts a floating point number (which is supported by senml) to a bytearray
func floatTimeToInt64(senmlTime float64) int64 {
	return int64(senmlTime * (1e9)) //time.Unix(int64(sec), int64(frac*(1e9))).UnixNano()
}

//This function converts a bytearray floating point number (which is supported by senml)
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

	switch stream.Type {
	case registry.Float:
		for rows.Next() {
			var val float64
			err = rows.Scan(&timeVal, &val)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			records = append(records, senml.Record{Name: senmlName, Value: &val, Time: timeVal})
		}
	case registry.String:
		for rows.Next() {
			var strVal string
			err = rows.Scan(&timeVal, &strVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			records = append(records, senml.Record{Name: senmlName, StringValue: strVal, Time: timeVal})
		}
	case registry.Bool:
		for rows.Next() {
			var boolVal bool
			err = rows.Scan(&timeVal, &boolVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			records = append(records, senml.Record{Name: senmlName, BoolValue: &boolVal, Time: timeVal})
		}
	case registry.Data:
		for rows.Next() {
			var dataVal string
			err = rows.Scan(&timeVal, &dataVal)
			if err != nil {
				return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			records = append(records, senml.Record{Name: senmlName, DataValue: dataVal, Time: timeVal})
		}
	}
	return records, total, nil
}

func (s *SqlStorage) queryMultipleStreams(q Query, streams ...*registry.DataStream) (pack senml.Pack, total *int, err error) {
	var unionStmt strings.Builder
	unionStmt.WriteByte('(')
	unionStr := ""

	streamMap := make(map[string]*registry.DataStream, len(streams))
	for _, stream := range streams {
		streamMap[stream.Name] = stream
		//unionStmt.WriteString(fmt.Sprintf("%s(SELECT ('%s' as 'table_name' , '%s' as 'type_val', 'time' as 'time', 'value' as 'value') FROM [%s] WHERE time BETWEEN %f and %f)", unionStr, stream.Name, stream.Type, stream.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))
		unionStmt.WriteString(fmt.Sprintf("%sSELECT  '%s' as 'table_name' , time, value FROM [%s] WHERE time BETWEEN %f and %f", unionStr, stream.Name, stream.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))
		unionStr = " UNION ALL "

	}
	unionStmt.WriteByte(')')

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
	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), q.Sort, q.PerPage, (q.Page-1)*q.PerPage)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("error while querying rows:%s", err)
	}
	defer rows.Close()

	records := make([]senml.Record, 0, q.PerPage)

	var tableName string
	var timeVal float64
	var val interface{}
	for rows.Next() {

		err = rows.Scan(&tableName, &timeVal, &val)
		if err != nil {
			return nil, nil, fmt.Errorf("error while scanning query results:%s", err)
		}
		switch streamMap[tableName].Type {
		case registry.Float:
			floatVal := val.(float64)
			records = append(records, senml.Record{Name: tableName, Value: &floatVal, Time: timeVal})
		case registry.String:
			stringVal := val.(string)
			records = append(records, senml.Record{Name: tableName, StringValue: stringVal, Time: timeVal})
		case registry.Bool:
			boolVal := val.(bool)
			records = append(records, senml.Record{Name: tableName, BoolValue: &boolVal, Time: timeVal})
		case registry.Data:
			dataVal := val.(string)
			records = append(records, senml.Record{Name: tableName, DataValue: dataVal, Time: timeVal})
		}
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

	stmt := fmt.Sprintf("CREATE TABLE [%s] (time DOUBLE, value %s)", ds.Name, typeVal[ds.Type])
	_, err := s.pool.Exec(stmt)
	return err
}

// UpdateHandler handles updates of a DataStream
func (s *SqlStorage) UpdateHandler(oldDS registry.DataStream, newDS registry.DataStream) error {
	//TODO supporting retetion

	return nil
}

// DeleteHandler handles deletion of a DataStream
func (s *SqlStorage) DeleteHandler(ds registry.DataStream) error {
	stmt := fmt.Sprintf("DROP TABLE [%s]", ds.Name)
	_, err := s.pool.Exec(stmt)
	return err
}
