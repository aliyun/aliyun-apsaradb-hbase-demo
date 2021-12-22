---
layout: news_item
date: "2018-09-10 08:30:00 +0000"
author: francischuang
version: 3.1.0
categories: [release]
tag: v3-1-0
sha: 0e1ae23
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

Apache Calcite Avatica Go 3.1.0 is a minor release of the Avatica Go client to bring in support for Go modules.
This release includes updated dependencies, testing against more targets and [support for Go Modules]({{ site.baseurl }}/docs/go_history.html#v3-1-0).

Go 1.11 along with Go modules support was released at the end of August 2018. Go modules will become the
official package management solution for Go projects. As the Go team currently supports both Go 1.11 and Go 1.10,
the Gopkg.toml and Gopkg.lock files are still available for those using dep for package management. We plan to
remove support for dep when Go 1.12 is released in early 2019, so we encourage users to upgrade to Go 1.11 and use
Go modules where possible.