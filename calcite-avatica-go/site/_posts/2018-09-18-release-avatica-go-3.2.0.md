---
layout: news_item
date: "2018-09-18 08:30:00 +0000"
author: francischuang
version: 3.2.0
categories: [release]
tag: v3-2-0
sha: 0a166d5
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

Apache Calcite Avatica Go 3.2.0 is a minor release of Avatica Go with fixes to the import paths after enabling
support for Go modules.

The 3.1.0 release contained a bug where packages within the library used the `"github.com/apache/calcite-avatica-go"`
import path rather than the `"github.com/apache/calcite-avatica-go/v3"` import path. This resulted in an issue where
2 versions of the library are being used at the same time, causing some programs to not build.

**The Calcite team recommends consumers of the Avatica Go library to not use the 3.1.0 release and ensure that the
3.2.0 release is being used.**