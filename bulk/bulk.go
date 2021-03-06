package bulk

import (
	"bytes"
	"database/sql"
	"strconv"

	"github.com/juju/errors"
)

type Bulk struct {
	db *sql.DB //Database handle
	tx *sql.Tx

	schema         string
	tableName      string
	columns        []string
	maxRowTxCommit int

	stmt   *sql.Stmt     //Prepared statement for bulk insert
	buf    []interface{} //Buffer to hold values to insert
	bufSz  int           //Size of the buffer
	bufPos int

	valuePtrs []interface{} //Pointer to current row buffer
	values    []interface{} //Buffer for the current row
	colCount  int           //Number of columns

	rowPos        int //Position of current row
	totalRowCount int //Total number of rows
}

//Appends row values to internal buffer
func (r *Bulk) Append(rows *sql.Rows) (err error) {
	rows.Scan(r.valuePtrs)

	//Copy row values into buffer
	for i := 0; i < r.colCount; i++ {
		r.buf[r.bufPos] = r.values[i]
		r.bufPos++
	}

	r.rowPos++
	r.totalRowCount++

	if r.totalRowCount > 0 && r.totalRowCount%r.maxRowTxCommit == 0 {
		if err = r.tx.Commit(); err != nil {
			return errors.Trace(err)
		}
		r.tx = nil
	}

	//Insert rows if buffer is full
	if r.bufPos >= r.bufSz {
		if r.tx == nil {
			if r.tx, err = r.db.Begin(); err != nil {
				return errors.Trace(err)
			}
		}

		if _, err = r.stmt.Exec(r.buf...); err != nil {
			return errors.Trace(err)
		}

		r.bufPos = 0
		r.rowPos = 0
	}

	return nil
}

//Closes any prepared statements
func (r *Bulk) Close() (err error) {
	if r.stmt != nil {
		r.stmt.Close()
	}

	return nil
}

//Writes any unsaved values from buffer to database
func (r *Bulk) Flush() (totalRowCount int, err error) {
	if r.bufPos > 0 {
		buf := make([]interface{}, r.bufPos)
		for i := 0; i < r.bufPos; i++ {
			buf[i] = r.buf[i]
		}

		stmt, err := r.prepare(r.rowPos)
		if err != nil {
			return 0, errors.Trace(err)
		}

		defer stmt.Close()

		if _, err = stmt.Exec(buf...); err != nil {
			return 0, errors.Trace(err)
		}

		r.totalRowCount += r.rowPos

		r.bufPos = 0
		r.rowPos = 0
	}

	if err = r.tx.Commit(); err != nil {
		return 0, errors.Trace(err)
	}

	return r.totalRowCount, nil
}

//Creates a bulk insert SQL prepared statement based on a number of rows
//Uses $x for row position
func (r *Bulk) prepare(rowCount int) (stmt *sql.Stmt, err error) {
	var buf bytes.Buffer

	buf.WriteString("INSERT INTO ")
	buf.WriteString(r.schema)
	buf.WriteString(".")
	buf.WriteString(r.tableName)
	buf.WriteString(" (")
	for i := 0; i < r.colCount; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(r.columns[i])
	}
	buf.WriteString(") VALUES ")

	pos := 1

	for i := 0; i < rowCount; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		for j := 0; j < r.colCount; j++ {
			if j > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(pos))
			pos++
		}
		buf.WriteString(")")
	}

	return r.db.Prepare(buf.String())
}

func NewBulk(db *sql.DB, columns []string, schema string, tableName string, rowCount int, maxRowTxCommit int) (r *Bulk, err error) {
	r = &Bulk{
		db:             db,
		schema:         schema,
		tableName:      tableName,
		columns:        columns,
		maxRowTxCommit: maxRowTxCommit}

	r.colCount = len(columns)

	r.values = make([]interface{}, r.colCount)
	r.valuePtrs = make([]interface{}, r.colCount)

	for i := 0; i < r.colCount; i++ {
		r.valuePtrs[i] = &r.values[i]
	}

	r.bufSz = r.colCount * rowCount
	r.bufPos = 0
	r.rowPos = 0

	r.buf = make([]interface{}, r.bufSz)

	if r.stmt, err = r.prepare(rowCount); err != nil {
		return nil, errors.Trace(err)
	}

	return r, nil
}
