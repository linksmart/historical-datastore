package data

import (
	"database/sql"
	"fmt"
	"strings"

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

func (s *SqlStorage) Query(q Query, sources ...*registry.DataStream) (pack senml.Pack, total int, nextOffset *int, err error) {

	//perItems, offsets := common.PerItemPagination(q.Limit, q.Page, q.PerPage, len(sources))

	//for index, source := range sources {
		partStmt := fmt.Sprintf("FROM [%s] WHERE time ORDER BY time %s LIMIT %d OFFSET %d", source.Name, q.Sort, perItems[index], offsets[index])
		//return nil, 0, nil, fmt.Errorf("not implemented")
		if q.count {
			stmt := "SELECT COUNT(*) " + partStmt
			row := s.pool.QueryRow(stmt)
			err := row.Scan(&total)
			if err != nil {
				return nil, 0, nil, fmt.Errorf("error while querying count:%s", err)
			}
		}


		stmt := fmt.Sprintf("SELECT (time, value) FROM [%s] WHERE time ORDER BY time %s LIMIT %d OFFSET %d ",
			ds, strings.Join(valueStrings, ","))
		_, err := s.pool.Exec(stmt, valueArgs...)
	}

	return err
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
