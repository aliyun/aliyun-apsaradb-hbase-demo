---
layout: news_item
date: "2019-05-16 08:30:00 +0000"
author: francischuang
version: 4.0.0
categories: [release]
tag: v4-0-0
sha: 3790ef5
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

Apache Calcite Avatica Go 4.0.0 is a major release with numerous improvements and a breaking change for Apache Phoenix.
As this is a new major version, users of this package will need to upgrade their import paths to 
`"github.com/apache/calcite-avatica-go/v4"`.

**Breaking change for Apache Phoenix ([CALCITE-2763](https://issues.apache.org/jira/browse/CALCITE-2724)):** 
In Apache Phoenix, null and empty strings are equivalent. For some background on why this is the case, see
[PHOENIX-947](https://issues.apache.org/jira/browse/PHOENIX-947). In version 3 of Avatica-Go and below, null and empty
strings are returned as an empty string `""` to the client. This prevented database/sql's built in NullString type from
working correctly. From 4.0.0 onwards, null and empty strings will be returned as a `nil`. This allows the usage of the
`sql.NullString` type.

For this release, both [dep](https://github.com/golang/dep) and Go modules are supported for package management. As 
Go modules will be turned on by default in Go 1.13.0 (estimated to be released in September/October 2019), it is highly
recommended that users of this package start migrating to using Go modules to ease the transition.

The Calcite team recommends users of this package to upgrade to this version, where practical, as the dependencies being
used by this package have also been upgraded.