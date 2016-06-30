package bulk

import (
  "database/sql"
  "strings"
  "bytes"
  "strconv"
)

type Bulk struct {
  db *sql.DB //Database handle
  tableName string
  columns []string


  stmt *sql.Stmt //Prepared statement for bulk insert
  buf []interface{} //Buffer to hold values to insert
  bufSz int //Size of the buffer
  bufPos int

  ValuePtrs []interface{} //Pointer to current row buffer
  values []interface{} //Buffer for the current row
  colCount int //Number of columns

  rowPos int //Position of current row
  TotalRowCount int //Total number of rows
}

//Appends row values to internal buffer
func (r *Bulk) Append() (err error) {
  //Copy row values into buffer
  for i := 0; i < r.colCount; i++ {
    r.buf[r.bufPos] = r.values[i]
    r.bufPos++
  }

  r.rowPos++

  //Insert rows if buffer is full
  if r.bufPos >= r.bufSz {
    if _, err = r.stmt.Exec(r.buf...); err != nil {
      return err
    }

    r.TotalRowCount += r.rowPos

    r.bufPos = 0
    r.rowPos = 0
  }

  return nil
}

//Closes any prepared statements
func (r *Bulk) Close() {
  if r.stmt != nil {
    r.stmt.Close()
  }
}

//Writes any unsaved values from buffer to database
func (r *Bulk) Flush() (err error) {
  if r.bufPos > 0 {
    buf := make([]interface{}, r.bufPos)
    for i := 0; i < r.bufPos; i++ {
      buf[i] = r.buf[i]
    }

    stmt, err := r.prepare(r.rowPos)
    if err != nil {
      return err
    }

    defer stmt.Close()

    if _, err = stmt.Exec(buf...); err != nil {
      return err
    }

    r.TotalRowCount += r.rowPos

    r.bufPos = 0
    r.rowPos = 0
  }

  return nil
}

//Creates a bulk insert SQL prepared statement based on a number of rows
//Uses $x for row position
func (r *Bulk) prepare(rowCount int) (stmt *sql.Stmt, err error) {
  var buf bytes.Buffer

  buf.WriteString("INSERT INTO ")
  buf.WriteString(r.tableName)
  buf.WriteString(" (")
  buf.WriteString(strings.Join(r.columns, ","))
  buf.WriteString(") VALUES ")

  pos := 1

  for i := 0; i < rowCount; i++ {
    if i > 0 {
      buf.WriteString(",");
    }
    buf.WriteString("(");
    for j := 0; j < r.colCount; j++ {
      if j > 0 {
        buf.WriteString(",")
      }
      buf.WriteString("$")
      buf.WriteString(strconv.Itoa(pos))
      pos++
    }
    buf.WriteString(")");
  }

  return r.db.Prepare(buf.String())
}

func NewBulk(db *sql.DB, columns []string, tableName string, rowCount int) (r *Bulk, err error) {
  r = &Bulk{
    db: db,
    tableName: tableName,
    columns: columns}

  r.colCount = len(columns)

  r.values = make([]interface{}, r.colCount)
  r.ValuePtrs = make([]interface{}, r.colCount)

  for i := 0; i < r.colCount; i++ {
      r.ValuePtrs[i] = &r.values[i]
  }

  r.bufSz = r.colCount * rowCount
  r.bufPos = 0
  r.rowPos = 0

  r.buf = make([]interface{}, r.bufSz)

  if r.stmt, err = r.prepare(rowCount); err != nil {
    return nil, err
  }

  return r, nil
}
