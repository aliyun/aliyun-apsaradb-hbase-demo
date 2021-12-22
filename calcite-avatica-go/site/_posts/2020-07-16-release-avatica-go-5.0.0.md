---
layout: news_item
date: "2020-07-16 08:30:00 +0000"
author: francischuang
version: 5.0.0
categories: [release]
tag: v5-0-0
sha: 0e3f5df
component: avatica-go
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

Apache Calcite Avatica Go 5.0.0 is a major release with numerous improvements and a breaking change.
As this is a new major version, users of this package will need to upgrade their import paths to 
`"github.com/apache/calcite-avatica-go/v5"`.

Since Go modules have been available since Go 1.11 (3 versions back as of writing), users of this library should 
install it using Go modules as support for dep has been removed.

This release also introduces the `batching` query string parameter in the DSN, which allows updates to the server using
a prepared statement to be batched together and executed once `Close()` is called on the statement.

**Breaking change for connection metadata ([CALCITE-3248](https://issues.apache.org/jira/browse/CALCITE-3248)):** 
Previously, it is possible to set the HTTP username and password using the `username` and `password` parameters in the
query string of the DSN. These parameters were confusing and didn't signal the intent and effect of the parameters in addition
to clashing with the `avaticaUser` and `avaticaPassword` parameters. The `username` and `password` parameters have now been
removed as CALCITE-3248 implements the [Connector interface](https://golang.org/pkg/database/sql/driver/#Connector) via the
`NewConnector()` method, which allows the driver to be instantiated with a custom HTTP client. Subsequently, it is now
possible to set up the driver with a custom HTTP client and decorate it with the `WithDigestAuth()`, `WithBasicAuth()`,
`WithKerberosAuth()` and `WithAdditionalHeaders()` methods.

The Calcite team recommends users of this package to upgrade to this version, where practical, as the dependencies being
used by this package have also been upgraded.