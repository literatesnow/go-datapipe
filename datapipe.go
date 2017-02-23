package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"

	"github.com/juju/errors"

	"github.com/literatesnow/go-datapipe/bulk"
)

type Config struct {
	maxRowBufSz    int //Maximum number of rows to buffer at a time
	maxRowTxCommit int //Maximum number of rows to process before committing the database transaction

	srcDbDriver  string //Source database driver name
	srcDbUri     string //Source database driver URI
	srcSelectSql string //Source database select SQL statement

	dstDbDriver string //Destination database driver name
	dstDbUri    string //Destination database driver URI
	dstSchema   string
	dstTable    string //Destination database table name

	showStackTrace bool //Display stack traces on error
}

type Insert interface {
	Append(rows *sql.Rows) (err error)
	Flush() (totalRowCount int, err error)
	Close() (err error)
}

func (c *Config) Init() (err error) {
	if os.Getenv("SHOW_STACK_TRACE") != "" {
		c.showStackTrace = true
	}

	c.maxRowBufSz, _ = c.EnvInt("MAX_ROW_BUF_SZ", 100)
	c.maxRowTxCommit, _ = c.EnvInt("MAX_ROW_TX_COMMIT", 500)

	if c.srcDbDriver, err = c.EnvStr("SRC_DB_DRIVER"); err != nil {
		return errors.Trace(err)
	}
	if c.srcDbUri, err = c.EnvStr("SRC_DB_URI"); err != nil {
		return errors.Trace(err)
	}
	if c.srcSelectSql, err = c.EnvStr("SRC_DB_SELECT_SQL"); err != nil {
		return errors.Trace(err)
	}

	if c.dstDbDriver, err = c.EnvStr("DST_DB_DRIVER"); err != nil {
		return errors.Trace(err)
	}
	if c.dstDbUri, err = c.EnvStr("DST_DB_URI"); err != nil {
		return errors.Trace(err)
	}
	if c.dstSchema, err = c.EnvStr("DST_DB_SCHEMA"); err != nil {
		return errors.Trace(err)
	}
	if c.dstTable, err = c.EnvStr("DST_DB_TABLE"); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *Config) EnvStr(envName string) (dst string, err error) {
	dst = os.Getenv(envName)
	if dst == "" {
		err = errors.Errorf("Missing ENV variable: %s", envName)
	}

	return dst, err
}

func (c *Config) EnvInt(envName string, defaultValue int) (dst int, err error) {
	if dst, err = strconv.Atoi(os.Getenv(envName)); err != nil {
		dst = defaultValue
	}

	return dst, nil
}

func main() {
	cfg := &Config{}
	if err := cfg.Init(); err != nil {
		showError(cfg, err)
		return
	}

	if err := run(cfg); err != nil {
		showError(cfg, err)
		return
	}
}

func run(cfg *Config) (err error) {
	var srcDb, dstDb *sql.DB

	if srcDb, err = sql.Open(cfg.srcDbDriver, cfg.srcDbUri); err != nil {
		return errors.Trace(err)
	}

	defer srcDb.Close()

	if dstDb, err = sql.Open(cfg.dstDbDriver, cfg.dstDbUri); err != nil {
		return errors.Trace(err)
	}

	defer dstDb.Close()

	if err = clearTable(dstDb, cfg); err != nil {
		return errors.Trace(err)
	}

	if err = copyTable(srcDb, dstDb, cfg); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func clearTable(dstDb *sql.DB, cfg *Config) (err error) {
	if _, err = dstDb.Exec("TRUNCATE TABLE " + cfg.dstSchema + "." + cfg.dstTable); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func copyTable(srcDb *sql.DB, dstDb *sql.DB, cfg *Config) (err error) {
	var ir Insert
	var rows *sql.Rows
	var rowCount int
	var columns []string

	if rows, err = srcDb.Query(cfg.srcSelectSql); err != nil {
		return errors.Trace(err)
	}

	defer rows.Close()

	if columns, err = rows.Columns(); err != nil {
		return errors.Trace(err)
	}

	startTime := time.Now()

	switch cfg.dstDbDriver {
	case "postgres":
		if ir, err = bulk.NewCopyIn(dstDb, columns, cfg.dstSchema, cfg.dstTable); err != nil {
			return errors.Trace(err)
		}
	default:
		if ir, err = bulk.NewBulk(dstDb, columns,
			cfg.dstSchema, cfg.dstTable,
			cfg.maxRowBufSz, cfg.maxRowTxCommit); err != nil {
			return errors.Trace(err)
		}
	}

	rowCount, err = copyBulkRows(dstDb, rows, ir, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if err = ir.Close(); err != nil {
		return errors.Trace(err)
	}

	fmt.Printf("%d rows in %s\n", rowCount, time.Since(startTime).String())

	return errors.Trace(rows.Err())
}

func copyBulkRows(dstDb *sql.DB, rows *sql.Rows, ir Insert, cfg *Config) (rowCount int, err error) {
	var totalRowCount int
	const dotLimit = 1000

	i := 1

	for rows.Next() {
		if err = ir.Append(rows); err != nil {
			return 0, errors.Trace(err)
		}

		if i%dotLimit == 0 {
			fmt.Print(".")
			i = 1
		}

		i++
	}

	if totalRowCount, err = ir.Flush(); err != nil {
		return 0, errors.Trace(err)
	}

	if totalRowCount > dotLimit {
		fmt.Println()
	}

	return totalRowCount, errors.Trace(rows.Err())
}

func showError(cfg *Config, err error) {
	if cfg.showStackTrace {
		fmt.Println(errors.ErrorStack(err))
	} else {
		fmt.Println(err)
	}
}
