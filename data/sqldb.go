package data

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/farshidtz/senml"
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
		case common.FLOAT:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, *r.Value)
			}
		case common.STRING:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, r.StringValue)
			}
		case common.BOOL:
			for _, r := range pack {
				valueStrings = append(valueStrings, "(?, ?)")
				valueArgs = append(valueArgs, r.Time, btoi(*r.BoolValue))
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

func (s *SqlStorage) Query(q Query, sources ...*registry.DataStream) (pack senml.Pack, total int, nextOffset *int, err error) {

	//perItems, offsets := common.PerItemPagination(q.Limit, q.Page, q.PerPage, len(sources))
	var unionStmt strings.Builder
	if len(sources) == 1 {
		unionStmt.WriteString(fmt.Sprintf("[%s] where time BETWEEN %d and %d LIMIT %d OFFSET %d",
			sources[0].Name, ToSenmlTime(q.From), ToSenmlTime(q.To), q.PerPage, (q.Page-1)*q.PerPage, q.Sort))
	} else {
		unionStmt.WriteByte('(')
		unionStr := ""
		for _, source := range sources {
			unionStmt.WriteString(fmt.Sprintf("%sSELECT ([%s] as table_name , %s as type, time, value) FROM [%s] WHERE time BETWEEN %d and %d", unionStr, source.Name, source.Type, source.Name, ToSenmlTime(q.From), ToSenmlTime(q.To)))
			unionStr = " UNION ALL "
		}
		unionStmt.WriteByte(')')
	}
	var stmt string
	if q.count {
		stmt = "SELECT COUNT(*) FROM " + unionStmt.String()
		row := s.pool.QueryRow(stmt)
		err := row.Scan(&total)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("error while querying count:%s", err)
		}
	}
	stmt = fmt.Sprintf("SELECT * FROM %s  ORDER BY time %s LIMIT %d OFFSET %d", unionStmt.String(), q.Sort, q.PerPage, (q.Page-1)*q.PerPage)
	rows, err := s.pool.Query(stmt)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("error while querying rows:%s", err)
	}
	defer rows.Close()
	records := make([]senml.Record, q.PerPage)
	if len(sources) == 1 {
		var timeVal float64
		switch sources[0].Type {
		case common.FLOAT:
			floatVal := val.(float64)
			records = append(records, senml.Record{Name: tableName, Value: &floatVal, Time: timeVal})
		case common.STRING:
			stringVal := val.(string)
			records = append(records, senml.Record{Name: tableName, StringValue: stringVal, Time: timeVal})
		case common.BOOL:
			boolVal := val.(bool)
			records = append(records, senml.Record{Name: tableName, BoolValue: boolVal, Time: timeVal})
		}
		for rows.Next() {
			err = rows.Scan(&timeVal, &val)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
		}
	} else {
		var tableName string
		var timeVal float64
		var val interface{}
		var typestr string
		for rows.Next() {

			err = rows.Scan(&tableName, &typestr, &timeVal, &val)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("error while scanning query results:%s", err)
			}
			switch typestr {
			case common.FLOAT:
				floatVal := val.(float64)
				records = append(records, senml.Record{Name: tableName, Value: &floatVal, Time: timeVal})
			case common.STRING:
				stringVal := val.(string)
				records = append(records, senml.Record{Name: tableName, StringValue: stringVal, Time: timeVal})
			case common.BOOL:
				boolVal := val.(bool)
				records = append(records, senml.Record{Name: tableName, BoolValue: boolVal, Time: timeVal})
			}
		}
	}
}

func (s *SqlStorage) Disconnect() error {
	return s.pool.Close()
}

// CreateHandler handles the creation of a new DataStream
func (s *SqlStorage) CreateHandler(ds registry.DataStream) error {
	var typeVal string
	switch ds.Type {
	case common.FLOAT:
		typeVal = "DOUBLE"
	case common.STRING:
		typeVal = "TEXT"
	case common.BOOL:
		typeVal = "BOOLEAN"
	}
	stmt := fmt.Sprintf("CREATE TABLE [%s] (time DOUBLE, value %s)", ds.Name, typeVal)
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
	//log.Println("LightdbStorage: dropped measurements for", ds.Name)
	return nil
}
