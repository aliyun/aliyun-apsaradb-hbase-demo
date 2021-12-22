// +build go1.8

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
	"database/sql"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestPhoenixContext(t *testing.T) {

	skipTestIfNotPhoenix(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExecContext(getContext(), "CREATE TABLE "+dbt.tableName+" (id BIGINT PRIMARY KEY, val VARCHAR) TRANSACTIONAL=false")

		dbt.mustExecContext(getContext(), "UPSERT INTO "+dbt.tableName+" VALUES (1,'A')")

		dbt.mustExecContext(getContext(), "UPSERT INTO "+dbt.tableName+" VALUES (2,'B')")

		rows := dbt.mustQueryContext(getContext(), "SELECT COUNT(*) FROM "+dbt.tableName)
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

		// Test transactions and prepared statements
		_, err := dbt.db.BeginTx(getContext(), &sql.TxOptions{Isolation: sql.LevelReadUncommitted, ReadOnly: true})

		if err == nil {
			t.Error("Expected an error while creating a read only transaction, but no error was returned")
		}

		tx, err := dbt.db.BeginTx(getContext(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})

		if err != nil {
			t.Errorf("Unexpected error while creating transaction: %s", err)
		}

		stmt, err := tx.PrepareContext(getContext(), "UPSERT INTO "+dbt.tableName+" VALUES(?,?)")

		if err != nil {
			t.Errorf("Unexpected error while preparing statement: %s", err)
		}

		res, err := stmt.ExecContext(getContext(), 3, "C")

		if err != nil {
			t.Errorf("Unexpected error while executing statement: %s", err)
		}

		affected, err := res.RowsAffected()

		if err != nil {
			t.Errorf("Error getting affected rows: %s", err)
		}

		if affected != 1 {
			t.Errorf("Expected 1 affected row, got %d", affected)
		}

		err = tx.Commit()

		if err != nil {
			t.Errorf("Error committing transaction: %s", err)
		}

		stmt2, err := dbt.db.PrepareContext(getContext(), "SELECT * FROM "+dbt.tableName+" WHERE id = ?")

		if err != nil {
			t.Errorf("Error preparing statement: %s", err)
		}

		row := stmt2.QueryRowContext(getContext(), 3)

		if err != nil {
			t.Errorf("Error querying for row: %s", err)
		}

		var (
			queryID  int64
			queryVal string
		)

		err = row.Scan(&queryID, &queryVal)

		if err != nil {
			t.Errorf("Error scanning results into variable: %s", err)
		}

		if queryID != 3 {
			t.Errorf("Expected scanned id to be %d, got %d", 3, queryID)
		}

		if queryVal != "C" {
			t.Errorf("Expected scanned string to be %s, got %s", "C", queryVal)
		}
	})
}

func TestPhoenixMultipleResultSets(t *testing.T) {

	skipTestIfNotPhoenix(t)

	runTests(t, dsn, func(dbt *DBTest) {
		// Create and seed table
		dbt.mustExecContext(getContext(), "CREATE TABLE "+dbt.tableName+" (id BIGINT PRIMARY KEY, val VARCHAR) TRANSACTIONAL=false")

		dbt.mustExecContext(getContext(), "UPSERT INTO "+dbt.tableName+" VALUES (1,'A')")

		dbt.mustExecContext(getContext(), "UPSERT INTO "+dbt.tableName+" VALUES (2,'B')")

		rows, err := dbt.db.QueryContext(getContext(), "SELECT * FROM "+dbt.tableName+" WHERE id = 1")

		if err != nil {
			t.Errorf("Unexpected error while executing query: %s", err)
		}

		defer rows.Close()

		for rows.Next() {
			var (
				id  int64
				val string
			)

			if err := rows.Scan(&id, &val); err != nil {
				t.Errorf("Error while scanning row into variables: %s", err)
			}

			if id != 1 {
				t.Errorf("Expected id to be %d, got %d", 1, id)
			}

			if val != "A" {
				t.Errorf("Expected value to be %s, got %s", "A", val)
			}
		}

		if rows.NextResultSet() {
			t.Error("There should be no more result sets, but got another result set")
		}
	})
}

func TestPhoenixColumnTypes(t *testing.T) {

	skipTestIfNotPhoenix(t)

	runTests(t, dsn, func(dbt *DBTest) {

		// Create and seed table
		dbt.mustExec(`CREATE TABLE ` + dbt.tableName + ` (
				int INTEGER PRIMARY KEY,
				uint UNSIGNED_INT,
				bint BIGINT,
				ulong UNSIGNED_LONG,
				tint TINYINT,
				utint UNSIGNED_TINYINT,
				sint SMALLINT,
				usint UNSIGNED_SMALLINT,
				flt FLOAT,
				uflt UNSIGNED_FLOAT,
				dbl DOUBLE,
				udbl UNSIGNED_DOUBLE,
				dec DECIMAL(10, 5),
				dec2 DECIMAL,
				bool BOOLEAN,
				tm TIME,
				dt DATE,
				tmstmp TIMESTAMP,
				utm UNSIGNED_TIME,
				udt UNSIGNED_DATE,
				utmstmp UNSIGNED_TIMESTAMP,
				var VARCHAR(10),
				ch CHAR(3),
				bin BINARY(20),
				varbin VARBINARY
			    ) TRANSACTIONAL=false`)

		// Select
		rows, err := dbt.db.QueryContext(getContext(), "SELECT * FROM "+dbt.tableName)

		if err != nil {
			t.Errorf("Unexpected error while selecting from table: %s", err)
		}

		columnNames, err := rows.Columns()

		if err != nil {
			t.Errorf("Error getting column names: %s", err)
		}

		expectedColumnNames := []string{"INT", "UINT", "BINT", "ULONG", "TINT", "UTINT", "SINT", "USINT", "FLT", "UFLT", "DBL", "UDBL", "DEC", "DEC2", "BOOL", "TM", "DT", "TMSTMP", "UTM", "UDT", "UTMSTMP", "VAR", "CH", "BIN", "VARBIN"}

		if !reflect.DeepEqual(columnNames, expectedColumnNames) {
			t.Error("Column names does not match expected column names")
		}

		type decimalSize struct {
			precision int64
			scale     int64
			ok        bool
		}

		type length struct {
			length int64
			ok     bool
		}

		type nullable struct {
			nullable bool
			ok       bool
		}

		expectedColumnTypes := []struct {
			databaseTypeName string
			decimalSize      decimalSize
			length           length
			name             string
			nullable         nullable
			scanType         reflect.Type
		}{
			{
				databaseTypeName: "INTEGER",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "INT",
				nullable: nullable{
					nullable: false,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_INT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "BIGINT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "BINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_LONG",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "ULONG",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "TINYINT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "TINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_TINYINT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UTINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "SMALLINT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "SINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_SMALLINT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "USINT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(int64(0)),
			},
			{
				databaseTypeName: "FLOAT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "FLT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(float64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_FLOAT",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UFLT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(float64(0)),
			},
			{
				databaseTypeName: "DOUBLE",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "DBL",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(float64(0)),
			},
			{
				databaseTypeName: "UNSIGNED_DOUBLE",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UDBL",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(float64(0)),
			},
			{
				databaseTypeName: "DECIMAL",
				decimalSize: decimalSize{
					precision: 10,
					scale:     5,
					ok:        true,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "DEC",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(""),
			},
			{
				databaseTypeName: "DECIMAL",
				decimalSize: decimalSize{
					precision: math.MaxInt64,
					scale:     math.MaxInt64,
					ok:        true,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "DEC2",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(""),
			},
			{
				databaseTypeName: "BOOLEAN",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "BOOL",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(bool(false)),
			},
			{
				databaseTypeName: "TIME",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "TM",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "DATE",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "DT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "TIMESTAMP",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "TMSTMP",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "UNSIGNED_TIME",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UTM",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "UNSIGNED_DATE",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UDT",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "UNSIGNED_TIMESTAMP",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 0,
					ok:     false,
				},
				name: "UTMSTMP",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(time.Time{}),
			},
			{
				databaseTypeName: "VARCHAR",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 10,
					ok:     true,
				},
				name: "VAR",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(""),
			},
			{
				databaseTypeName: "CHAR",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 3,
					ok:     true,
				},
				name: "CH",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf(""),
			},
			{
				databaseTypeName: "BINARY",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: 20,
					ok:     true,
				},
				name: "BIN",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf([]byte{}),
			},
			{
				databaseTypeName: "VARBINARY",
				decimalSize: decimalSize{
					precision: 0,
					scale:     0,
					ok:        false,
				},
				length: length{
					length: math.MaxInt64,
					ok:     true,
				},
				name: "VARBIN",
				nullable: nullable{
					nullable: true,
					ok:       true,
				},
				scanType: reflect.TypeOf([]byte{}),
			},
		}

		columnTypes, err := rows.ColumnTypes()

		if err != nil {
			t.Errorf("Error getting column types: %s", err)
		}

		for index, columnType := range columnTypes {

			expected := expectedColumnTypes[index]

			if columnType.DatabaseTypeName() != expected.databaseTypeName {
				t.Errorf("Expected database type name for index %d to be %s, got %s", index, expected.databaseTypeName, columnType.DatabaseTypeName())
			}

			precision, scale, ok := columnType.DecimalSize()

			if precision != expected.decimalSize.precision {
				t.Errorf("Expected decimal precision for index %d to be %d, got %d", index, expected.decimalSize.precision, precision)
			}

			if scale != expected.decimalSize.scale {
				t.Errorf("Expected decimal scale for index %d to be %d, got %d", index, expected.decimalSize.scale, scale)
			}

			if ok != expected.decimalSize.ok {
				t.Errorf("Expected decimal ok for index %d to be %t, got %t", index, expected.decimalSize.ok, ok)
			}

			length, ok := columnType.Length()

			if length != expected.length.length {
				t.Errorf("Expected length for index %d to be %d, got %d", index, expected.length.length, length)
			}

			if ok != expected.length.ok {
				t.Errorf("Expected length ok for index %d to be %t, got %t", index, expected.length.ok, ok)
			}

			if columnType.Name() != expected.name {
				t.Errorf("Expected column name for index %d to be %s, got %s", index, expected.name, columnType.Name())
			}

			nullable, ok := columnType.Nullable()

			if nullable != expected.nullable.nullable {
				t.Errorf("Expected nullable for index %d to be %t, got %t", index, expected.nullable.nullable, nullable)
			}

			if ok != expected.nullable.ok {
				t.Errorf("Expected nullable ok for index %d to be %t, got %t", index, expected.nullable.ok, ok)
			}

			if columnType.ScanType() != expected.scanType {
				t.Errorf("Expected scan type for index %d to be %s, got %s", index, expected.scanType, columnType.ScanType())
			}
		}

	})
}
