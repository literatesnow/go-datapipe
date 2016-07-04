package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"

	"github.com/lic-nz/go-datapipe/bulk"
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
}

type Insert interface {
	Append(rows *sql.Rows) (err error)
	Flush() (totalRowCount int, err error)
	Close() (err error)
}

func (c *Config) Init() (err error) {
	c.maxRowBufSz, _ = c.EnvInt("MAX_ROW_BUF_SZ", 100)
	c.maxRowTxCommit, _ = c.EnvInt("MAX_ROW_TX_COMMIT", 500)

	if c.srcDbDriver, err = c.EnvStr("SRC_DB_DRIVER"); err != nil {
		return err
	}
	if c.srcDbUri, err = c.EnvStr("SRC_DB_URI"); err != nil {
		return err
	}
	if c.srcSelectSql, err = c.EnvStr("SRC_DB_SELECT_SQL"); err != nil {
		return err
	}

	if c.dstDbDriver, err = c.EnvStr("DST_DB_DRIVER"); err != nil {
		return err
	}
	if c.dstDbUri, err = c.EnvStr("DST_DB_URI"); err != nil {
		return err
	}
	if c.dstSchema, err = c.EnvStr("DST_DB_SCHEMA"); err != nil {
		return err
	}
	if c.dstTable, err = c.EnvStr("DST_DB_TABLE"); err != nil {
		return err
	}

	return nil
}

func (c *Config) EnvStr(envName string) (dst string, err error) {
	dst = os.Getenv(envName)
	if dst == "" {
		err = errors.New("Missing ENV variable: " + envName)
	} else {
		log.Printf("%s=%s", envName, dst)
	}

	return dst, err
}

func (c *Config) EnvInt(envName string, defaultValue int) (dst int, err error) {
	if dst, err = strconv.Atoi(os.Getenv(envName)); err != nil {
		dst = defaultValue
	}

	log.Printf("%s=%d", envName, dst)

	return dst, nil
}

func main() {
	cfg := &Config{}
	if err := cfg.Init(); err != nil {
		log.Fatal(err)
		return
	}

	if err := run(cfg); err != nil {
		log.Fatal(err)
		return
	}
}

func run(cfg *Config) (err error) {
	var srcDb, dstDb *sql.DB

	if srcDb, err = sql.Open(cfg.srcDbDriver, cfg.srcDbUri); err != nil {
		return err
	}

	defer srcDb.Close()

	if dstDb, err = sql.Open(cfg.dstDbDriver, cfg.dstDbUri); err != nil {
		return err
	}

	defer dstDb.Close()

	if err = clearTable(dstDb, cfg); err != nil {
		return err
	}

	if err = copyTable(srcDb, dstDb, cfg); err != nil {
		return err
	}

	return nil
}

func clearTable(dstDb *sql.DB, cfg *Config) (err error) {
	if _, err = dstDb.Exec("TRUNCATE TABLE " + cfg.dstSchema + "." + cfg.dstTable); err != nil {
		return err
	}
	return nil
}

func copyTable(srcDb *sql.DB, dstDb *sql.DB, cfg *Config) (err error) {
	var ir Insert
	var rows *sql.Rows
	var rowCount int
	var columns []string

	if rows, err = srcDb.Query(cfg.srcSelectSql); err != nil {
		return err
	}

	if columns, err = rows.Columns(); err != nil {
		return err
	}

	startTime := time.Now()

	switch cfg.dstDbDriver {
	case "postgres":
		log.Printf("Using COPY IN\n")
		if ir, err = bulk.NewCopyIn(dstDb, columns, cfg.dstSchema, cfg.dstTable); err != nil {
			return err
		}
	default:
		if ir, err = bulk.NewBulk(dstDb, columns, cfg.dstSchema, cfg.dstTable, cfg.maxRowBufSz, cfg.maxRowTxCommit); err != nil {
			return err
		}
	}

	rowCount, err = copyBulkRows(dstDb, rows, ir, cfg)

	if err = ir.Close(); err != nil {
		return err
	}

	duration := time.Since(startTime)

	log.Printf("%d rows in %s\n", rowCount, duration.String())

	return err
}

func copyBulkRows(dstDb *sql.DB, rows *sql.Rows, ir Insert, cfg *Config) (rowCount int, err error) {
	var totalRowCount int

	i := 1

	for rows.Next() {
		if err = ir.Append(rows); err != nil {
			return 0, err
		}

		if i%1000 == 0 {
			fmt.Print(".")
		}

		i++
	}

	if totalRowCount, err = ir.Flush(); err != nil {
		return 0, err
	}

	fmt.Println()

	return totalRowCount, nil
}
