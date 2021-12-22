---
layout: news_item
date: "2018-04-27 08:30:00 +0000"
author: francischuang
version: 3.0.0
categories: [release]
tag: v3-0-0
sha: 273c53f
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

Apache Calcite Avatica Go 3.0.0 is the first release of the Avatica [Go](https://golang.org/)
[database/sql](https://golang.org/pkg/database/sql/) driver since the code has been donated to the Apache Calcite
project. This release includes support for Avatica with the HSQLDB backend, updated dependencies and [bug fixes]({{ site.baseurl }}/docs/go_history.html#v3-0-0).

There is a breaking change where the `Name()` method on the `ResponseError` error type has been changed to a property, `Name`.

Users of the current `Boostport/avatica` library are encouraged to update to this new version of `apache/calcite-avatica-go`
as further development will take place in the `apache/calcite-avatica-go` repository.

For most users, the updating is simply replacing the import path `_ github.com/Boostport/avatica` with `_ github.com/apache/calcite-avatica-go`.