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
	"strconv"
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {

	_, config, err := ParseDSN("http://localhost:8765/myschema?maxRowsTotal=1&frameMaxSize=1&location=Australia/Melbourne&transactionIsolation=8&authentication=BASIC&avaticaUser=someuser&avaticaPassword=somepassword")

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if config.endpoint != "http://localhost:8765/myschema" {
		t.Errorf("Expected endpoint to be %s, got %s", "http://localhost:8765/myschema", config.endpoint)
	}

	if config.frameMaxSize != 1 {
		t.Errorf("Expected frameMaxSize to be %d, got %d", 1, config.frameMaxSize)
	}

	if config.maxRowsTotal != 1 {
		t.Errorf("Expected maxRowsTotal to be %d, got %d", 1, config.maxRowsTotal)
	}

	if config.location.String() != "Australia/Melbourne" {
		t.Errorf("Expected timezone to be %s, got %s", "Australia/Melbourne", config.location)
	}

	if config.schema != "myschema" {
		t.Errorf("Expected schema to be %s, got %s", "myschema", config.schema)
	}

	if config.transactionIsolation != 8 {
		t.Errorf("Expected transactionIsolation to be %d, got %d", 8, config.transactionIsolation)
	}

	if config.authentication != basic {
		t.Errorf("Expected authentication to be BASIC (%d) got %d", basic, config.authentication)
	}

	if config.avaticaUser != "someuser" {
		t.Errorf("Expected avaticaUser to be %s, got %s", "someuser", config.avaticaUser)
	}

	if config.avaticaPassword != "somepassword" {
		t.Errorf("Expected avaticaPassword to be %s, got %s", "somepassword", config.avaticaPassword)
	}
}

func TestParseDSNProxy(t *testing.T) {

	_, config, err := ParseDSN("http://localhost:8765/service/proxy/myschema?authentication=BASIC&avaticaUser=someuser&avaticaPassword=somepassword")

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if config.endpoint != "http://localhost:8765/service/proxy/myschema" {
		t.Errorf("Expected endpoint to be %s, got %s", "http://localhost:8765/service/proxy/myschema", config.endpoint)
	}

	if config.schema != "myschema" {
		t.Errorf("Expected schema to be %s, got %s", "myschema", config.schema)
	}

	if config.authentication != basic {
		t.Errorf("Expected authentication to be BASIC (%d) got %d", basic, config.authentication)
	}

	if config.avaticaUser != "someuser" {
		t.Errorf("Expected avaticaUser to be %s, got %s", "someuser", config.avaticaUser)
	}

	if config.avaticaPassword != "somepassword" {
		t.Errorf("Expected avaticaPassword to be %s, got %s", "somepassword", config.avaticaPassword)
	}
}

func TestParseEmptyDSN(t *testing.T) {

	_, _, err := ParseDSN("")

	if err == nil {
		t.Fatal("Expected error due to empty DSN, but received nothing")
	}
}

func TestDSNDefaults(t *testing.T) {

	_, config, err := ParseDSN("http://localhost:8765")

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if config.location.String() == "" {
		t.Error("There was no timezone set.")
	}

	if config.maxRowsTotal == 0 {
		t.Error("There was no maxRowsTotal set.")
	}

	if config.frameMaxSize == 0 {
		t.Error("There was no fetchMaxSize set.")
	}

	if config.schema != "" {
		t.Errorf("Unexpected schema set: %s", config.schema)
	}

	if config.transactionIsolation != 0 {
		t.Errorf("Default transaction level should be %d, got %d.", 0, config.transactionIsolation)
	}

	if config.authentication != none {
		t.Errorf("Default authentication should be NONE (%d), got %d", none, config.authentication)
	}

	if config.avaticaUser != "" {
		t.Errorf("Default avaticaUser should be empty, got %s", config.avaticaUser)
	}

	if config.avaticaPassword != "" {
		t.Errorf("Default avaticaPassword should be empty, got %s", config.avaticaPassword)
	}

	principal := krb5Principal{}

	if config.principal != principal {
		t.Errorf("Default principal should be empty, got %s", config.principal)
	}

	if config.keytab != "" {
		t.Errorf("Default keytab should be empty, got %s", config.keytab)
	}

	if config.krb5Conf != "" {
		t.Errorf("Default krb5Conf should be empty, got %s", config.krb5Conf)
	}

	if config.krb5CredentialCache != "" {
		t.Errorf("Default krb5CredentialCache should be empty, got %s", config.krb5CredentialCache)
	}
}

func TestDSNDefaultsProxy(t *testing.T) {

	_, config, err := ParseDSN("http://localhost:8765/service/proxy/")

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if config.location.String() == "" {
		t.Error("There was no timezone set.")
	}

	if config.maxRowsTotal == 0 {
		t.Error("There was no maxRowsTotal set.")
	}

	if config.frameMaxSize == 0 {
		t.Error("There was no fetchMaxSize set.")
	}

	if config.schema != "" {
		t.Errorf("Unexpected schema set: %s", config.schema)
	}

	if config.transactionIsolation != 0 {
		t.Errorf("Default transaction level should be %d, got %d.", 0, config.transactionIsolation)
	}

	if config.authentication != none {
		t.Errorf("Default authentication should be NONE (%d), got %d", none, config.authentication)
	}

	if config.avaticaUser != "" {
		t.Errorf("Default avaticaUser should be empty, got %s", config.avaticaUser)
	}

	if config.avaticaPassword != "" {
		t.Errorf("Default avaticaPassword should be empty, got %s", config.avaticaPassword)
	}

	principal := krb5Principal{}

	if config.principal != principal {
		t.Errorf("Default principal should be empty, got %s", config.principal)
	}

	if config.keytab != "" {
		t.Errorf("Default keytab should be empty, got %s", config.keytab)
	}

	if config.krb5Conf != "" {
		t.Errorf("Default krb5Conf should be empty, got %s", config.krb5Conf)
	}

	if config.krb5CredentialCache != "" {
		t.Errorf("Default krb5CredentialCache should be empty, got %s", config.krb5CredentialCache)
	}
}

func TestLocalLocation(t *testing.T) {

	_, config, err := ParseDSN("http://localhost:8765?location=Local")

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if config.location != time.Local {
		t.Fatal("DSN has location set to 'local', but configuration did not set location to 'local'.")
	}
}

func TestBadInput(t *testing.T) {

	_, _, err := ParseDSN("http://localhost:8765?location=asdfasdf")

	if err == nil {
		t.Fatal("Expected error due to invalid location, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?maxRowsTotal=abc")

	if err == nil {
		t.Fatal("Expected error due to invalid maxRowsTotal, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?frameMaxSize=abc")

	if err == nil {
		t.Fatal("Expected error due to invalid frameMaxSize, but did not receive any.")
	}
}

func TestInvalidTransactionIsolation(t *testing.T) {

	badIsolationLevels := []int{-1, 3, 5, 6, 7, 9, 10, 11, 100}

	for _, isolationLevel := range badIsolationLevels {

		_, _, err := ParseDSN("http://localhost:8765?transactionIsolation=" + strconv.Itoa(isolationLevel))

		if err == nil {
			t.Fatal("Expected error due to invalid transactionIsolation, but did not receive any.")
		}
	}
}

func TestValidTransactionIsolation(t *testing.T) {

	validIsolationLevels := []int{0, 1, 2, 4, 8}

	for _, isolationLevel := range validIsolationLevels {

		_, _, err := ParseDSN("http://localhost:8765?transactionIsolation=" + strconv.Itoa(isolationLevel))

		if err != nil {
			t.Fatalf("Unexpected error when %d is set as the isolation level: %s", isolationLevel, err)
		}
	}
}

func TestInvalidAuthentication(t *testing.T) {

	_, _, err := ParseDSN("http://localhost:8765?authentication=ASDF")

	if err == nil {
		t.Fatal("Expected error due to invalid authentication, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=BASIC")

	if err == nil {
		t.Fatal("Expected error due to missing avaticaUser and avaticaPassword, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=BASIC&avaticaUser=test")

	if err == nil {
		t.Fatal("Expected error due to missing avaticaPassword, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=BASIC&avaticaPassword=test")

	if err == nil {
		t.Fatal("Expected error due to missing avaticaUser, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO&principal=test/test@realm&krb5Conf=/path/to/krb5.conf")

	if err == nil {
		t.Fatal("Expected error due to missing keytab, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO&keytab=/path/to/file.keytab&krb5Conf=/path/to/krb5.conf")

	if err == nil {
		t.Fatal("Expected error due to missing principal, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO&principal=test/test@realm&keytab=/path/to/file.keytab")

	if err == nil {
		t.Fatal("Expected error due to missing krb5Conf, but did not receive any.")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO")

	if err == nil {
		t.Fatal("Expected error due to invalid SPNEGO _,config, but did not receive any.")
	}
}

func TestValidAuthentication(t *testing.T) {
	_, _, err := ParseDSN("http://localhost:8765?authentication=BASIC&avaticaUser=test&avaticaPassword=test")

	if err != nil {
		t.Fatal("Unexpected error when DSN contains an authentication method, avaticaUser and avaticaPassword")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=DIGEST&avaticaUser=test&avaticaPassword=test")

	if err != nil {
		t.Fatal("Unexpected error when DSN contains an authentication method, avaticaUser and avaticaPassword")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO&principal=test/test@realm&keytab=/path/to/file.keytab&krb5Conf=/path/to/krb5.conf")

	if err != nil {
		t.Fatal("Unexpected error when DSN contains an authentication method, principal and keytab and krb5Conf")
	}

	_, _, err = ParseDSN("http://localhost:8765?authentication=SPNEGO&krb5CredentialCache=/path/to/cache")

	if err != nil {
		t.Fatal("Unexpected error when DSN contains an authentication method with path to the credential cache")
	}
}
