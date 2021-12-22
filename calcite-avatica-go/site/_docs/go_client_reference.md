---
layout: docs
title: Go Client Reference
sidebar_title: Go Client Reference
permalink: /docs/go_client_reference.html
---

<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

The Avatica Go client is an Avatica driver for Go's
[database/sql](https://golang.org/pkg/database/sql/)package.

It also works with the Phoenix Query Server from the Apache
Phoenix project, as the Phoenix Query Server uses Avatica under the
hood.

* TOC
{:toc}

## Getting Started
Install using Go modules:

{% highlight shell %}
$ go get github.com/apache/calcite-avatica-go
{% endhighlight %}


## Usage

The Avatica Go driver implements Go's `database/sql/driver` interface, so, import Go's
`database/sql` package and the driver:

{% highlight go %}
import "database/sql"
import _ "github.com/apache/calcite-avatica-go/v5"

db, err := sql.Open("avatica", "http://localhost:8765")
{% endhighlight %}

Then simply use the database connection to query some data, for example:

{% highlight go %}
rows := db.Query("SELECT COUNT(*) FROM test")
{% endhighlight %}

## DSN (Data Source Name)

The DSN has the following format (optional parts are marked by square brackets):

{% highlight shell %}
http://address:port[/schema][?parameter1=value&...parameterN=value]
{% endhighlight %}

In other words, the scheme (http), address and port are mandatory, but the schema and parameters are optional.

{% comment %}
It's a shame that we have to embed HTML to get the anchors but the normal
header tags from kramdown screw up the definition list. We lose the pretty
on-hover images for the permalink, but oh well.
{% endcomment %}

<strong><a name="schema" href="#schema">schema</a></strong>

The `schema` path sets the default schema to use for this connection. For example, if you set it to `myschema`,
then executing the query `SELECT * FROM my_table` will have the equivalence of `SELECT * FROM myschema.my_table`.
If schema is set, you can still work on tables in other schemas by supplying a schema prefix:
`SELECT * FROM myotherschema.my_other_table`.

The parameters references the options used by the Java implementation as much as possible.
The following parameters are supported:

<strong><a name="authentication" href="#authentication">authentication</a></strong>

The authentication type to use when authenticating against Avatica. Valid values are `BASIC` for HTTP Basic authentication,
`DIGEST` for HTTP Digest authentication, and `SPNEGO` for Kerberos with SPNEGO authentication.

<strong><a name="avaticaUser" href="#avaticaUser">avaticaUser</a></strong>

The user to use when authenticating against Avatica. This parameter is required if `authentication` is `BASIC` or `DIGEST`.

<strong><a name="avaticaPassword" href="#avaticaPassword">avaticaPassword</a></strong>

The password to use when authenticating against Avatica. This parameter is required if `authentication` is `BASIC` or `DIGEST`.

<strong><a name="principal" href="#principal">principal</a></strong>

The Kerberos principal to use when authenticating against Avatica. It should be in the form `primary/instance@realm`, where
the instance is optional. This parameter is required if `authentication` is `SPNEGO` and you want the driver to perform the
Kerberos login.

<strong><a name="keytab" href="#keytab">keytab</a></strong>

The path to the Kerberos keytab to use when authenticating against Avatica. This parameter is required if `authentication`
is `SPNEGO` and you want the driver to perform the Kerberos login.

<strong><a name="krb5Conf" href="#krb5Conf">krb5Conf</a></strong>

The path to the Kerberos configuration to use when authenticating against Avatica. This parameter is required if `authentication`
is `SPNEGO` and you want the driver to perform the Kerberos login.

<strong><a name="krb5CredentialsCache" href="#krb5CredentialsCache">krb5CredentialsCache</a></strong>

The path to the Kerberos credential cache file to use when authenticating against Avatica. This parameter is required if
`authentication` is `SPNEGO` and you have logged into Kerberos already and want the driver to use the existing credentials.

<strong><a name="location" href="#location">location</a></strong>

The `location` will be set as the location of unserialized `time.Time` values. It must be a valid timezone.
If you want to use the local timezone, use `Local`. By default, this is set to `UTC`.

<strong><a name="maxRowsTotal" href="#maxRowsTotal">maxRowsTotal</a></strong>

The `maxRowsTotal` parameter sets the maximum number of rows to return for a given query. By default, this is set to
`-1`, so that there is no limit on the number of rows returned.

<strong><a name="frameMaxSize" href="#frameMaxSize">frameMaxSize</a></strong>

The `frameMaxSize` parameter sets the maximum number of rows to return in a frame. Depending on the number of rows
returned and subject to the limits of `maxRowsTotal`, a query result set can contain rows in multiple frames. These
additional frames are then fetched on a as-needed basis. `frameMaxSize` allows you to control the number of rows
in each frame to suit your application's performance profile. By default this is set to `-1`, so that there is no limit
on the number of rows in a frame.

<strong><a name="transactionIsolation" href="#transactionIsolation">transactionIsolation</a></strong>

Setting `transactionIsolation` allows you to set the isolation level for transactions using the connection. The value
should be a positive integer analogous to the transaction levels defined by the JDBC specification. The default value
is `0`, which means transactions are not supported. This is to deal with the fact that Calcite/Avatica works with
many types of backends, with some backends having no transaction support. If you are using Apache Phoenix 4.7 onwards,
we recommend setting it to `4`, which is the maximum isolation level supported.

The supported values for `transactionIsolation` are:

| Value | JDBC Constant                  | Description                                                                      |
| :-----| :----------------------------- | :------------------------------------------------------------------------------- |
| 0     | none                           | Transactions are not supported                                                   |
| 1     | `TRANSACTION_READ_UNCOMMITTED` | Dirty reads, non-repeatable reads and phantom reads may occur.                   |
| 2     | `TRANSACTION_READ_COMMITTED`   | Dirty reads are prevented, but non-repeatable reads and phantom reads may occur. |
| 4     | `TRANSACTION_REPEATABLE_READ`  | Dirty reads and non-repeatable reads are prevented, but phantom reads may occur. |
| 8     | `TRANSACTION_SERIALIZABLE`     | Dirty reads, non-repeatable reads, and phantom reads are all prevented.          |

<strong><a name="batching" href="#batching">batching</a></strong>

When you want to write large amounts of data, you can enable batching rather than making a call to the server for each execution.
By using [ExecuteBatchRequest](https://calcite.apache.org/avatica/docs/protobuf_reference.html#executebatchrequest), 
the driver will batch up `Exec()`s and send them to the sever when a statement is closed using `Close()`. The statement object
is thread-safe and can be used by multiple go-routines, but the changes will only be sent to the server after the
statement has been closed.

```go
// when using phoenix
stmt, _ := db.Prepare(`UPSERT INTO ` + dbt.tableName + ` VALUES(?)`)
var wg sync.WaitGroup
for i := 1; i <= 6; i++ {
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

// When batching=true, the statement will only be executed when Close() is called
err = stmt.Close()
```

## time.Time support

The following datatypes are automatically converted to and from `time.Time`:
`TIME`, `DATE` and `TIMESTAMP`.

It is important to understand that Avatica and the underlying database ignores the timezone. If you save a `time.Time`
to the database, the timezone is ignored and vice-versa. This is why you need to make sure the `location` parameter
in your DSN is set to the same value as the location of the `time.Time` values you are inserting into the database.

We recommend using `UTC`, which is the default value of `location`.

## Apache Phoenix Error Codes
The Go client comes with support for retrieving the error code when an error occurs. This is extremely useful when
you want to take specific action when a particular type of error occurs.

If the error returned is a ResponseError, the `Name` field on the error will return the appropriate
Apache Phoenix error code:

{% highlight go %}
_, err := db.Exec("SELECT * FROM table_that_does_not_exist") // Query undefined table

// First, assert the error type
perr, ok := err.(avatica.ResponseError)

// If it cannot be asserted
if !ok {
    // Error was not an Avatica ResponseError
}

// Print the Apache Phoenix error code
fmt.Println(perr.Name) // Prints: table_undefined
{% endhighlight %}

## Version Compatibility

| Driver Version  | Phoenix Version   | Calcite-Avatica Version |
| :-------------- | :---------------- | :---------------------- |
| 3.x.x           | >= 4.8.0          | >= 1.11.0               |
