package bulk

import (
	"database/sql"

	"github.com/lib/pq"
)

type CopyIn struct {
	db *sql.DB //Database handle
	tx *sql.Tx

	stmt *sql.Stmt

	valuePtrs []interface{} //Pointer to current row buffer
	values    []interface{} //Buffer for the current row

	totalRowCount int //Total number of rows
}

//Appends row values to internal buffer
func (r *CopyIn) Append(rows *sql.Rows) (err error) {
	rows.Scan(r.valuePtrs...)

	//TODO Ensure proper data type

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

func (r *CopyIn) Flush() (totalRowCount int, err error) {
	if _, err = r.stmt.Exec(); err != nil {
		return 0, err
	}

	return r.totalRowCount, nil
}

func NewCopyIn(db *sql.DB, columns []string, schema string, tableName string) (r *CopyIn, err error) {
	r = &CopyIn{
		db: db}

	colCount := len(columns)

	r.values = make([]interface{}, colCount)
	r.valuePtrs = make([]interface{}, colCount)

	for i := 0; i < colCount; i++ {
		r.valuePtrs[i] = &r.values[i]
	}

	if r.tx, err = r.db.Begin(); err != nil {
		return nil, err
	}

	if r.stmt, err = r.tx.Prepare(pq.CopyInSchema(schema, tableName, columns...)); err != nil {
		return nil, err
	}

	return r, nil
}
