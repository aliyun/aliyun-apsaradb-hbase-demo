/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package avatica

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func skipTestIfNotHSQLDB(t *testing.T) {

	val := os.Getenv("AVATICA_FLAVOR")

	if val != "HSQLDB" {
		t.Skip("Skipping Apache Avatica HSQLDB test")
	}
}

func TestHSQLDBConnectionMustBeOpenedWithAutoCommitTrue(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec("CREATE TABLE " + dbt.tableName + " (id BIGINT PRIMARY KEY, val VARCHAR(1))")

		dbt.mustExec("INSERT INTO " + dbt.tableName + " VALUES (1,'A')")

		dbt.mustExec("INSERT INTO " + dbt.tableName + " VALUES (2,'B')")

		rows := dbt.mustQuery("SELECT COUNT(*) FROM " + dbt.tableName)
		defer rows.Close()

		for rows.Next() {

			var count int

			err := rows.Scan(&count)

			if err != nil {
				dbt.Fatal(err)
			}

			if count != 2 {
				dbt.Fatalf("There should be 2 rows, got %d", count)
			}
		}

	})
}

func TestHSQLDBZeroValues(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec("CREATE TABLE " + dbt.tableName + " (int INTEGER PRIMARY KEY, flt FLOAT, bool BOOLEAN, str VARCHAR(1))")

		dbt.mustExec("INSERT INTO " + dbt.tableName + " VALUES (0, 0.0, false, '')")

		rows := dbt.mustQuery("SELECT * FROM " + dbt.tableName)
		defer rows.Close()

		for rows.Next() {

			var i int
			var flt float64
			var b bool
			var s string

			err := rows.Scan(&i, &flt, &b, &s)

			if err != nil {
				dbt.Fatal(err)
			}

			if i != 0 {
				dbt.Fatalf("Integer should be 0, got %v", i)
			}

			if flt != 0.0 {
				dbt.Fatalf("Float should be 0.0, got %v", flt)
			}

			if b != false {
				dbt.Fatalf("Boolean should be false, got %v", b)
			}

			if s != "" {
				dbt.Fatalf("String should be \"\", got %v", s)
			}
		}

	})
}

func TestHSQLDBDataTypes(t *testing.T) {

	// TODO; Test case for Time type is currently commented out due to CALCITE-1951

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		/*dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
			int INTEGER PRIMARY KEY,
			tint TINYINT,
			sint SMALLINT,
			bint BIGINT,
			num NUMERIC(10,3),
			dec DECIMAL(10,3),
			re REAL,
			flt FLOAT,
			dbl DOUBLE,
			bool BOOLEAN,
			ch CHAR(3),
			var VARCHAR(128),
			bin BINARY(20),
			varbin VARBINARY(128),
			dt DATE,
			tm TIME,
			tmstmp TIMESTAMP,
		    )`)*/

		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY,
				tint TINYINT,
				sint SMALLINT,
				bint BIGINT,
				num NUMERIC(10,3),
				dec DECIMAL(10,3),
				re REAL,
				flt FLOAT,
				dbl DOUBLE,
				bool BOOLEAN,
				ch CHAR(3),
				var VARCHAR(128),
				bin BINARY(20),
				varbin VARBINARY(128),
				dt DATE,
				tmstmp TIMESTAMP,
			    )`)

		var (
			integerValue int       = -20
			tintValue    int       = -128
			sintValue    int       = -32768
			bintValue    int       = -9223372036854775807
			numValue     string    = "1.333"
			decValue     string    = "1.333"
			reValue      float64   = 3.555
			fltValue     float64   = -3.555
			dblValue     float64   = -9.555
			booleanValue bool      = true
			chValue      string    = "a"
			varcharValue string    = "test string"
			binValue     []byte    = make([]byte, 20, 20)
			varbinValue  []byte    = []byte("testtesttest")
			dtValue      time.Time = time.Date(2100, 2, 1, 0, 0, 0, 0, time.UTC)
			// tmValue      time.Time = time.Date(0, 1, 1, 21, 21, 21, 222000000, time.UTC)
			tmstmpValue time.Time = time.Date(2100, 2, 1, 21, 21, 21, 222000000, time.UTC)
		)

		copy(binValue[:], []byte("test"))

		// dbt.mustExec(`INSERT INTO `+dbt.tableName+` (int, tint, sint, bint, num, dec, re, flt, dbl, bool, ch, var, bin, varbin, dt, tm, tmstmp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		dbt.mustExec(`INSERT INTO `+dbt.tableName+` (int, tint, sint, bint, num, dec, re, flt, dbl, bool, ch, var, bin, varbin, dt, tmstmp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			integerValue,
			tintValue,
			sintValue,
			bintValue,
			numValue,
			decValue,
			reValue,
			fltValue,
			dblValue,
			booleanValue,
			chValue,
			varcharValue,
			binValue,
			varbinValue,
			dtValue,
			// tmValue,
			tmstmpValue,
		)

		rows := dbt.mustQuery("SELECT * FROM " + dbt.tableName)
		defer rows.Close()

		var (
			integer int
			tint    int
			sint    int
			bint    int
			num     string
			dec     string
			re      float64
			flt     float64
			dbl     float64
			boolean bool
			ch      string
			varchar string
			bin     []byte
			varbin  []byte
			dt      time.Time
			// tm      time.Time
			tmstmp time.Time
		)

		for rows.Next() {

			// err := rows.Scan(&integer, &tint, &sint, &bint, &num, &dec, &re, &flt, &dbl, &boolean, &ch, &varchar, &bin, &varbin, &dt, &tm, &tmstmp)
			err := rows.Scan(&integer, &tint, &sint, &bint, &num, &dec, &re, &flt, &dbl, &boolean, &ch, &varchar, &bin, &varbin, &dt, &tmstmp)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		comparisons := []struct {
			result   interface{}
			expected interface{}
		}{
			{integer, integerValue},
			{tint, tintValue},
			{sint, sintValue},
			{bint, bintValue},
			{num, numValue},
			{dec, decValue},
			{re, reValue},
			{flt, fltValue},
			{dbl, dblValue},
			{boolean, booleanValue},
			{ch, chValue + "  "}, // HSQLDB pads CHAR columns if a length is specified
			{varchar, varcharValue},
			{bin, binValue},
			{varbin, varbinValue},
			{dt, dtValue},
			//			{tm, tmValue},
			{tmstmp, tmstmpValue},
		}

		for _, tt := range comparisons {

			if v, ok := tt.expected.(time.Time); ok {

				if !v.Equal(tt.result.(time.Time)) {
					dbt.Fatalf("Expected %v, got %v.", tt.expected, tt.result)
				}

			} else if v, ok := tt.expected.([]byte); ok {

				if !bytes.Equal(v, tt.result.([]byte)) {
					dbt.Fatalf("Expected %v, got %v.", tt.expected, tt.result)
				}

			} else if tt.expected != tt.result {
				dbt.Errorf("Expected %v, got %v.", tt.expected, tt.result)
			}
		}
	})
}

func TestHSQLDBSQLNullTypes(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				id INTEGER PRIMARY KEY,
				int INTEGER,
				tint TINYINT,
				sint SMALLINT,
				bint BIGINT,
				num NUMERIC(10,3),
				dec DECIMAL(10,3),
				re REAL,
				flt FLOAT,
				dbl DOUBLE,
				bool BOOLEAN,
				ch CHAR(3),
				var VARCHAR(128),
				bin BINARY(20),
				varbin VARBINARY(128),
				dt DATE,
				tmstmp TIMESTAMP,
			    )`)

		var (
			idValue                 = time.Now().Unix()
			integerValue            = sql.NullInt64{}
			tintValue               = sql.NullInt64{}
			sintValue               = sql.NullInt64{}
			bintValue               = sql.NullInt64{}
			numValue                = sql.NullString{}
			decValue                = sql.NullString{}
			reValue                 = sql.NullFloat64{}
			fltValue                = sql.NullFloat64{}
			dblValue                = sql.NullFloat64{}
			booleanValue            = sql.NullBool{}
			chValue                 = sql.NullString{}
			varcharValue            = sql.NullString{}
			binValue     *[]byte    = nil
			varbinValue  *[]byte    = nil
			dtValue      *time.Time = nil
			tmstmpValue  *time.Time = nil
		)

		dbt.mustExec(`INSERT INTO `+dbt.tableName+` (id, int, tint, sint, bint, num, dec, re, flt, dbl, bool, ch, var, bin, varbin, dt, tmstmp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			idValue,
			integerValue,
			tintValue,
			sintValue,
			bintValue,
			numValue,
			decValue,
			reValue,
			fltValue,
			dblValue,
			booleanValue,
			chValue,
			varcharValue,
			binValue,
			varbinValue,
			dtValue,
			tmstmpValue,
		)

		rows := dbt.mustQuery("SELECT * FROM "+dbt.tableName+" WHERE id = ?", idValue)
		defer rows.Close()

		var (
			id      int64
			integer sql.NullInt64
			tint    sql.NullInt64
			sint    sql.NullInt64
			bint    sql.NullInt64
			num     sql.NullString
			dec     sql.NullString
			re      sql.NullFloat64
			flt     sql.NullFloat64
			dbl     sql.NullFloat64
			boolean sql.NullBool
			ch      sql.NullString
			varchar sql.NullString
			bin     *[]byte
			varbin  *[]byte
			dt      *time.Time
			tmstmp  *time.Time
		)

		for rows.Next() {
			err := rows.Scan(&id, &integer, &tint, &sint, &bint, &num, &dec, &re, &flt, &dbl, &boolean, &ch, &varchar, &bin, &varbin, &dt, &tmstmp)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		comparisons := []struct {
			result   interface{}
			expected interface{}
		}{
			{integer, integerValue},
			{tint, tintValue},
			{sint, sintValue},
			{bint, bintValue},
			{num, numValue},
			{dec, decValue},
			{re, reValue},
			{flt, fltValue},
			{dbl, dblValue},
			{boolean, booleanValue},
			{ch, chValue},
			{varchar, varcharValue},
			{bin, binValue},
			{varbin, varbinValue},
			{dt, dtValue},
			{tmstmp, tmstmpValue},
		}

		for i, tt := range comparisons {

			if v, ok := tt.expected.(time.Time); ok {

				if !v.Equal(tt.result.(time.Time)) {
					dbt.Fatalf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
				}

			} else if v, ok := tt.expected.([]byte); ok {

				if !bytes.Equal(v, tt.result.([]byte)) {
					dbt.Fatalf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
				}

			} else if tt.expected != tt.result {
				dbt.Errorf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
			}
		}
	})
}

func TestHSQLDBNulls(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				id INTEGER PRIMARY KEY,
				int INTEGER,
				tint TINYINT,
				sint SMALLINT,
				bint BIGINT,
				num NUMERIC(10,3),
				dec DECIMAL(10,3),
				re REAL,
				flt FLOAT,
				dbl DOUBLE,
				bool BOOLEAN,
				ch CHAR(3),
				var VARCHAR(128),
				bin BINARY(20),
				varbin VARBINARY(128),
				dt DATE,
				tmstmp TIMESTAMP,
			    )`)

		idValue := time.Now().Unix()

		dbt.mustExec(`INSERT INTO `+dbt.tableName+` (id, int, tint, sint, bint, num, dec, re, flt, dbl, bool, ch, var, bin, varbin, dt, tmstmp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			idValue,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)

		rows := dbt.mustQuery("SELECT * FROM "+dbt.tableName+" WHERE id = ?", idValue)
		defer rows.Close()

		var (
			id      int64
			integer sql.NullInt64
			tint    sql.NullInt64
			sint    sql.NullInt64
			bint    sql.NullInt64
			num     sql.NullString
			dec     sql.NullString
			re      sql.NullFloat64
			flt     sql.NullFloat64
			dbl     sql.NullFloat64
			boolean sql.NullBool
			ch      sql.NullString
			varchar sql.NullString
			bin     *[]byte
			varbin  *[]byte
			dt      *time.Time
			tmstmp  *time.Time
		)

		for rows.Next() {
			err := rows.Scan(&id, &integer, &tint, &sint, &bint, &num, &dec, &re, &flt, &dbl, &boolean, &ch, &varchar, &bin, &varbin, &dt, &tmstmp)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		comparisons := []struct {
			result   interface{}
			expected interface{}
		}{
			{integer, sql.NullInt64{}},
			{tint, sql.NullInt64{}},
			{sint, sql.NullInt64{}},
			{bint, sql.NullInt64{}},
			{num, sql.NullString{}},
			{dec, sql.NullString{}},
			{re, sql.NullFloat64{}},
			{flt, sql.NullFloat64{}},
			{dbl, sql.NullFloat64{}},
			{boolean, sql.NullBool{}},
			{ch, sql.NullString{}},
			{varchar, sql.NullString{}},
			{bin, (*[]byte)(nil)},
			{varbin, (*[]byte)(nil)},
			{dt, (*time.Time)(nil)},
			{tmstmp, (*time.Time)(nil)},
		}

		for i, tt := range comparisons {

			if v, ok := tt.expected.(time.Time); ok {

				if !v.Equal(tt.result.(time.Time)) {
					dbt.Fatalf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
				}

			} else if v, ok := tt.expected.([]byte); ok {

				if !bytes.Equal(v, tt.result.([]byte)) {
					dbt.Fatalf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
				}

			} else if tt.expected != tt.result {
				dbt.Errorf("Expected %v for case %d, got %v.", tt.expected, i, tt.result)
			}
		}
	})
}

// TODO: Test case commented out due to CALCITE-1951
/*func TestHSQLDBLocations(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?location=Australia/Melbourne"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				tm TIME(6) PRIMARY KEY,
				dt DATE,
				tmstmp TIMESTAMP
			    )`)

		loc, err := time.LoadLocation("Australia/Melbourne")

		if err != nil {
			dbt.Fatalf("Unexpected error: %s", err)
		}

		var (
			tmValue     time.Time = time.Date(0, 1, 1, 21, 21, 21, 222000000, loc)
			dtValue     time.Time = time.Date(2100, 2, 1, 0, 0, 0, 0, loc)
			tmstmpValue time.Time = time.Date(2100, 2, 1, 21, 21, 21, 222000000, loc)
		)

		dbt.mustExec(`INSERT INTO `+dbt.tableName+`(tm, dt, tmstmp) VALUES (?, ?, ?)`,
			tmValue,
			dtValue,
			tmstmpValue,
		)

		rows := dbt.mustQuery("SELECT * FROM " + dbt.tableName)
		defer rows.Close()

		var (
			tm     time.Time
			dt     time.Time
			tmstmp time.Time
		)

		for rows.Next() {

			err := rows.Scan(&tm, &dt, &tmstmp)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		comparisons := []struct {
			result   time.Time
			expected time.Time
		}{
			{tm, tmValue},
			{dt, dtValue},
			{tmstmp, tmstmpValue},
		}

		for _, tt := range comparisons {
			if !tt.result.Equal(tt.expected) {
				dbt.Errorf("Expected %v, got %v.", tt.expected, tt.result)
			}
		}
	})
}*/

func TestHSQLDBDateAndTimestampsBefore1970(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY,
				dt DATE,
				tmstmp TIMESTAMP
			    )`)

		var (
			integerValue int       = 1
			dtValue      time.Time = time.Date(1945, 5, 20, 0, 0, 0, 0, time.UTC)
			tmstmpValue  time.Time = time.Date(1911, 5, 20, 21, 21, 21, 222000000, time.UTC)
		)

		dbt.mustExec(`INSERT INTO `+dbt.tableName+`(int, dt, tmstmp) VALUES (?, ?, ?)`,
			integerValue,
			dtValue,
			tmstmpValue,
		)

		rows := dbt.mustQuery("SELECT dt, tmstmp FROM " + dbt.tableName)
		defer rows.Close()

		var (
			dt     time.Time
			tmstmp time.Time
		)

		for rows.Next() {
			err := rows.Scan(&dt, &tmstmp)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		comparisons := []struct {
			result   time.Time
			expected time.Time
		}{
			{dt, dtValue},
			{tmstmp, tmstmpValue},
		}

		for _, tt := range comparisons {
			if !tt.expected.Equal(tt.result) {
				dbt.Fatalf("Expected %v, got %v.", tt.expected, tt.result)
			}
		}
	})
}

func TestHSQLDBStoreAndRetrieveBinaryData(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		// TODO: Switch VARBINARY to BLOB once avatica supports BLOBs and CBLOBs. CALCITE-1957
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY,
				bin VARBINARY(999999)
			    )`)

		filePath := filepath.Join("test-fixtures", "calcite.png")

		file, err := ioutil.ReadFile(filePath)

		if err != nil {
			t.Fatalf("Unable to read text-fixture: %s", filePath)
		}

		hash := sha256.Sum256(file)

		dbt.mustExec(`INSERT INTO `+dbt.tableName+` (int, bin) VALUES (?, ?)`,
			1,
			file,
		)

		rows := dbt.mustQuery("SELECT bin FROM " + dbt.tableName)
		defer rows.Close()

		var receivedFile []byte

		for rows.Next() {

			err := rows.Scan(&receivedFile)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		receivedHash := sha256.Sum256(receivedFile)

		if !bytes.Equal(hash[:], receivedHash[:]) {
			t.Fatalf("Hash of stored file (%x) does not equal hash of retrieved file (%x).", hash[:], receivedHash[:])
		}
	})
}

func TestHSQLDBCommittingTransactions(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?transactionIsolation=4"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		tx, err := dbt.db.Begin()

		if err != nil {
			t.Fatalf("Unable to create transaction: %s", err)
		}

		stmt, err := tx.Prepare(`INSERT INTO ` + dbt.tableName + `(int) VALUES(?)`)

		if err != nil {
			t.Fatalf("Could not prepare statement: %s", err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		r := tx.QueryRow("SELECT COUNT(*) FROM " + dbt.tableName)

		var count int

		err = r.Scan(&count)

		if err != nil {
			t.Fatalf("Unable to scan row result: %s", err)
		}

		if count != totalRows {
			t.Fatalf("Expected %d rows, got %d", totalRows, count)
		}

		// Commit the transaction
		tx.Commit()

		rows := dbt.mustQuery("SELECT COUNT(*) FROM " + dbt.tableName)

		var countAfterRollback int

		for rows.Next() {
			err := rows.Scan(&countAfterRollback)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		if countAfterRollback != totalRows {
			t.Fatalf("Expected %d rows, got %d", totalRows, countAfterRollback)
		}
	})
}

func TestHSQLDBRollingBackTransactions(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?transactionIsolation=4"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		tx, err := dbt.db.Begin()

		if err != nil {
			t.Fatalf("Unable to create transaction: %s", err)
		}

		stmt, err := tx.Prepare(`INSERT INTO ` + dbt.tableName + `(int) VALUES(?)`)

		if err != nil {
			t.Fatalf("Could not prepare statement: %s", err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		r := tx.QueryRow(`SELECT COUNT(*) FROM ` + dbt.tableName)

		var count int

		err = r.Scan(&count)

		if err != nil {
			t.Fatalf("Unable to scan row result: %s", err)
		}

		if count != totalRows {
			t.Fatalf("Expected %d rows, got %d", totalRows, count)
		}

		// Rollback the transaction
		tx.Rollback()

		rows := dbt.mustQuery(`SELECT COUNT(*) FROM ` + dbt.tableName)

		var countAfterRollback int

		for rows.Next() {
			err := rows.Scan(&countAfterRollback)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		if countAfterRollback != 0 {
			t.Fatalf("Expected %d rows, got %d", 0, countAfterRollback)
		}
	})
}

func TestHSQLDBPreparedStatements(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + `(int) VALUES(?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		queryStmt, err := dbt.db.Prepare(`SELECT * FROM ` + dbt.tableName + ` WHERE int = ?`)

		if err != nil {
			dbt.Fatal(err)
		}

		var res int

		for i := 1; i <= totalRows; i++ {

			err := queryStmt.QueryRow(i).Scan(&res)

			if err != nil {
				dbt.Fatal(err)
			}

			if res != i {
				dbt.Fatalf("Unexpected query result. Expected %d, got %d.", i, res)
			}
		}
	})
}

func TestHSQLDBFetchingMoreRows(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?maxRowsTotal=-1&frameMaxSize=1"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + `(int) VALUES(?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		rows := dbt.mustQuery(`SELECT * FROM ` + dbt.tableName)
		defer rows.Close()

		count := 0

		for rows.Next() {
			count++
		}

		if count != totalRows {
			dbt.Fatalf("Expected %d rows to be retrieved, retrieved %d", totalRows, count)
		}
	})
}

func TestHSQLDBExecuteShortcut(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		res, err := dbt.db.Exec(`INSERT INTO ` + dbt.tableName + `(int) VALUES(1)`)

		if err != nil {
			dbt.Fatal(err)
		}

		affected, err := res.RowsAffected()

		if err != nil {
			dbt.Fatal(err)
		}

		if affected != 1 {
			dbt.Fatalf("Expected 1 row to be affected, %d affected", affected)
		}
	})
}

func TestHSQLDBQueryShortcut(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?maxRowsTotal=-1&frameMaxSize=1"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + `(int) VALUES(?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		rows := dbt.mustQuery(`SELECT * FROM ` + dbt.tableName)
		defer rows.Close()

		count := 0

		for rows.Next() {
			count++
		}

		if count != totalRows {
			dbt.Fatalf("Expected %d rows to be retrieved, retrieved %d", totalRows, count)
		}
	})
}

// TODO: Test disabled due to CALCITE-2250
/*func TestHSQLDBOptimisticConcurrency(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	query := "?transactionIsolation=4"

	runTests(t, dsn+query, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				id INTEGER PRIMARY KEY,
				msg VARCHAR(64),
				version INTEGER
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + `(id, msg, version) VALUES(?, ?, ?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i, fmt.Sprintf("message version %d", i), i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		// Start the transactions
		tx1, err := dbt.db.Begin()

		if err != nil {
			dbt.Fatal(err)
		}

		tx2, err := dbt.db.Begin()

		if err != nil {
			dbt.Fatal(err)
		}

		// Select from first transaction
		_ = tx1.QueryRow(`SELECT MAX(version) FROM ` + dbt.tableName)

		// Modify using second transaction
		_, err = tx2.Exec(`INSERT INTO `+dbt.tableName+`(id, msg, version) VALUES(?, ?, ?)`, 7, "message value 7", 7)

		if err != nil {
			dbt.Fatal(err)
		}

		err = tx2.Commit()

		if err != nil {
			dbt.Fatal(err)
		}

		// Modify using tx1
		_, err = tx1.Exec(`INSERT INTO `+dbt.tableName+`(id, msg, version) VALUES(?, ?, ?)`, 7, "message value 7", 7)

		if err != nil {
			dbt.Fatal(err)
		}

		err = tx1.Commit()

		if err == nil {
			dbt.Fatal("Expected an error, but did not receive any.")
		}

		errName := err.(ResponseError).Name()

		if errName != "transaction_conflict_exception" {
			dbt.Fatal("Expected transaction_conflict")
		}
	})
}*/

func TestHSQLDBLastInsertIDShouldReturnError(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	runTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				id INTEGER IDENTITY PRIMARY KEY,
				msg VARCHAR(3),
				version INTEGER
			    )`)

		res, err := dbt.db.Exec(`INSERT INTO ` + dbt.tableName + `(msg, version) VALUES('abc', 1)`)

		if err != nil {
			dbt.Fatal(err)
		}

		_, err = res.LastInsertId()

		if err == nil {
			dbt.Fatal("Expected an error as Avatica does not support LastInsertId(), but there was no error.")
		}
	})
}

func TestHSQLDBSchemaSupport(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	db, err := sql.Open("avatica", dsn)

	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer db.Close()

	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS avaticatest")

	if err != nil {
		t.Fatalf("error creating schema: %s", err)
	}

	defer db.Exec("DROP SCHEMA IF EXISTS avaticatest")

	path := "/avaticatest"

	runTests(t, dsn+path, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE avaticatest.` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    );`)

		defer dbt.mustExec(`DROP TABLE IF EXISTS avaticatest.` + dbt.tableName)

		_, err := dbt.db.Exec(`INSERT INTO avaticatest.` + dbt.tableName + `(int) VALUES(1)`)

		if err != nil {
			dbt.Fatal(err)
		}

		rows := dbt.mustQuery(`SELECT * FROM avaticatest.` + dbt.tableName)
		defer rows.Close()

		count := 0

		for rows.Next() {
			count++
		}

		if count != 1 {
			dbt.Errorf("Expected 1 row, got %d rows back,", count)
		}
	})
}

func TestHSQLDBMultipleSchemaSupport(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	db, err := sql.Open("avatica", dsn)

	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer db.Close()

	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS avaticatest1")

	if err != nil {
		t.Fatalf("error creating schema: %s", err)
	}

	defer db.Exec("DROP SCHEMA IF EXISTS avaticatest1")

	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS avaticatest2")

	if err != nil {
		t.Fatalf("error creating schema: %s", err)
	}

	defer db.Exec("DROP SCHEMA IF EXISTS avaticatest2")

	path := "/avaticatest1"

	runTests(t, dsn+path, func(dbt *DBTest) {

		dbt.mustExec(`SET INITIAL SCHEMA avaticatest2`)

		// Create and seed table
		dbt.mustExec(`CREATE TABLE avaticatest2.` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		defer dbt.mustExec(`DROP TABLE IF EXISTS avaticatest2.` + dbt.tableName)

		_, err := dbt.db.Exec(`INSERT INTO avaticatest2.` + dbt.tableName + `(int) VALUES(1)`)

		if err != nil {
			dbt.Fatal(err)
		}

		rows := dbt.mustQuery(`SELECT * FROM avaticatest2.` + dbt.tableName)
		defer rows.Close()

		count := 0

		for rows.Next() {
			count++
		}

		if count != 1 {
			dbt.Errorf("Expected 1 row, got %d rows back,", count)
		}
	})
}

func TestHSQLDBExecBatch(t *testing.T) {
	skipTestIfNotHSQLDB(t)

	runTests(t, dsn+"?batching=true", func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + ` VALUES(?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		for i := 1; i <= totalRows; i++ {
			_, err := stmt.Exec(i)

			if err != nil {
				dbt.Fatal(err)
			}
		}

		// When batching=true, after exec(sql), need to close the stmt
		err = stmt.Close()

		if err != nil {
			dbt.Fatal(err)
		}

		queryStmt, err := dbt.db.Prepare(`SELECT * FROM ` + dbt.tableName + ` WHERE int = ?`)

		if err != nil {
			dbt.Fatal(err)
		}

		var res int

		for i := 1; i <= totalRows; i++ {

			err := queryStmt.QueryRow(i).Scan(&res)

			if err != nil {
				dbt.Fatal(err)
			}

			if res != i {
				dbt.Fatalf("Unexpected query result. Expected %d, got %d.", i, res)
			}
		}
	})
}

func TestHSQLDBExecBatchConcurrency(t *testing.T) {
	skipTestIfNotHSQLDB(t)

	runTests(t, dsn+"?batching=true", func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY
			    )`)

		stmt, err := dbt.db.Prepare(`INSERT INTO ` + dbt.tableName + ` VALUES(?)`)

		if err != nil {
			dbt.Fatal(err)
		}

		totalRows := 6

		var wg sync.WaitGroup
		for i := 1; i <= totalRows; i++ {
			wg.Add(1)
			go func(num int) {
				defer wg.Done()

				_, err := stmt.Exec(num)

				if err != nil {
					dbt.Fatal(err)
				}
			}(i)
		}
		wg.Wait()

		// When batching=true, after exec(sql), need to close the stmt
		err = stmt.Close()

		if err != nil {
			dbt.Fatal(err)
		}

		queryStmt, err := dbt.db.Prepare(`SELECT * FROM ` + dbt.tableName + ` WHERE int = ?`)

		if err != nil {
			dbt.Fatal(err)
		}

		var res int

		for i := 1; i <= totalRows; i++ {

			err := queryStmt.QueryRow(i).Scan(&res)

			if err != nil {
				dbt.Fatal(err)
			}

			if res != i {
				dbt.Fatalf("Unexpected query result. Expected %d, got %d.", i, res)
			}
		}
	})
}

// TODO: Test disabled due to CALCITE-1049
/*func TestHSQLDBErrorCodeParsing(t *testing.T) {

	skipTestIfNotHSQLDB(t)

	db, err := sql.Open("avatica", dsn)

	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer db.Close()

	_, err = db.Query("SELECT * FROM table_that_does_not_exist")

	if err == nil {
		t.Error("Expected error due to selecting from non-existent table, but there was no error.")
	}

	resErr, ok := err.(ResponseError)

	if !ok {
		t.Fatalf("Error type was not ResponseError")
	}

	if resErr.ErrorCode != 1012 {
		t.Errorf("Expected error code to be %d, got %d.", 1012, resErr.ErrorCode)
	}

	if resErr.SqlState != "42M03" {
		t.Errorf("Expected SQL state to be %s, got %s.", "42M03", resErr.SqlState)
	}
}*/
