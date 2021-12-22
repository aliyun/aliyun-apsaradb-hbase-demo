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
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"
)

var (
	dsn string
)

func init() {

	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	var serverAddr string

	if val := os.Getenv("AVATICA_FLAVOR"); val == "PHOENIX" {
		serverAddr = env("PHOENIX_HOST", "http://phoenix:8765")
	} else if val == "HSQLDB" {
		serverAddr = env("HSQLDB_HOST", "http://hsqldb:8765")
	} else {
		panic("The AVATICA_FLAVOR environment variable should be either PHOENIX or HSQLDB")
	}

	dsn = serverAddr

	// Wait for the avatica server to be ready
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)
	ticker := time.NewTicker(2 * time.Second)

	for {

		select {
		case <-ctx.Done():
			panic("Timed out while waiting for the avatica server to be ready after 5 minutes.")

		case <-ticker.C:
			resp, err := http.Get(serverAddr)

			if err == nil {
				resp.Body.Close()
				ticker.Stop()
				return
			}
		}
	}
}

func generateTableName() string {
	return fmt.Sprintf("%s%d%d", "test", time.Now().UnixNano(), rand.Intn(100))
}

type DBTest struct {
	*testing.T
	db        *sql.DB
	tableName string
}

func (dbt *DBTest) fail(method, query string, err error) {

	if len(query) > 300 {
		query = "[query too large to print]"
	}

	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)

	if err != nil {
		dbt.fail("exec", query, err)
	}

	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)

	if err != nil {
		dbt.fail("query", query, err)
	}

	return rows
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {

	db, err := sql.Open("avatica", dsn)

	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer db.Close()

	table := generateTableName()

	db.Exec("DROP TABLE IF EXISTS " + table)

	dbt := &DBTest{t, db, table}

	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS " + table)
	}
}

func TestConnectionToInvalidServerShouldReturnError(t *testing.T) {

	runTests(t, "http://invalid-server:8765", func(dbt *DBTest) {

		_, err := dbt.db.Exec(`CREATE TABLE ` + dbt.tableName + ` (
					id INTEGER PRIMARY KEY,
					msg VARCHAR,
			    	      ) TRANSACTIONAL=false`)

		if err == nil {
			dbt.Fatal("Expected an error due to connection to invalid server, but got nothing.")
		}
	})
}
