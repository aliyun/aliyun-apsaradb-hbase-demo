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
	"context"
	"database/sql"
	"testing"
	"time"
)

func (dbt *DBTest) mustExecContext(ctx context.Context, query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.ExecContext(ctx, query, args...)

	if err != nil {
		dbt.fail("exec", query, err)
	}

	return res
}

func (dbt *DBTest) mustQueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.QueryContext(ctx, query, args...)

	if err != nil {
		dbt.fail("query", query, err)
	}

	return rows
}

func getContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 4*time.Minute)

	return ctx
}

func TestPing(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		err := dbt.db.Ping()

		if err != nil {
			t.Errorf("Expected ping to succeed, got error: %s", err)
		}
	})
}

func TestInvalidPing(t *testing.T) {
	runTests(t, "http://invalid-server:8765", func(dbt *DBTest) {
		err := dbt.db.Ping()

		if err == nil {
			t.Error("Expected ping to fail, but did not get any error")
		}
	})
}
