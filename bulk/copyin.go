package bulk

import (
	"database/sql"
	"strconv"

	"github.com/lib/pq"
)

type CopyIn struct {
	db *sql.DB //Database handle
	tx *sql.Tx

	stmt *sql.Stmt

	valueTypes []string

	valuePtrs []interface{} //Pointer to current row buffer
	values    []interface{} //Buffer for the current row

	totalRowCount int64 //Total number of rows
}

//Appends row values to internal buffer
func (r *CopyIn) Append(rows *sql.Rows) (err error) {
	rows.Scan(r.valuePtrs...)

	for i := 0; i < len(r.valueTypes); i++ {
		if r.values[i] == nil {
			continue
		}

		if s, ok := r.values[i].([]byte); ok {
			switch r.valueTypes[i] {
			case "numeric":
				r.values[i], _ = strconv.ParseFloat(string(s), 64)
			default:
				r.values[i] = string(s)
			}
		}
	}

	if _, err = r.stmt.Exec(r.values...); err != nil {
		return err
	}

	r.totalRowCount++

	return nil
}

//Closes any prepared statements
func (r *CopyIn) Close() (err error) {
	if err = r.stmt.Close(); err != nil {
		return err
	}

	if err = r.tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (r *CopyIn) Flush() (totalRowCount int64, err error) {
	if _, err = r.stmt.Exec(); err != nil {
		return 0, err
	}

	return r.totalRowCount, nil
}

func (r *CopyIn) findColumnTypes(schema string, tableName string, columns []string) (err error) {
	sql := "SELECT column_name AS name, data_type AS type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"

	rows, err := r.db.Query(sql, schema, tableName)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var colName, colType string

		if err := rows.Scan(&colName, &colType); err != nil {
			return err
		}

		for i := 0; i < len(columns); i++ {
			if colName == columns[i] {
				r.valueTypes[i] = colType
			}
		}
	}

	return rows.Err()
}

func NewCopyIn(db *sql.DB, columns []string, schema string, tableName string) (r *CopyIn, err error) {
	r = &CopyIn{
		db: db}

	colCount := len(columns)

	r.values = make([]interface{}, colCount)
	r.valuePtrs = make([]interface{}, colCount)
	r.valueTypes = make([]string, colCount)

	for i := 0; i < colCount; i++ {
		r.valuePtrs[i] = &r.values[i]
	}

	if r.tx, err = r.db.Begin(); err != nil {
		return nil, err
	}

	if err = r.findColumnTypes(schema, tableName, columns); err != nil {
		return nil, err
	}

	if r.stmt, err = r.tx.Prepare(pq.CopyInSchema(schema, tableName, columns...)); err != nil {
		return nil, err
	}

	return r, nil
}
