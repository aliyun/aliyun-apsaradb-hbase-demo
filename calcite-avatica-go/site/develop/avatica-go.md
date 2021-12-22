---
layout: page
title: Developing the Avatica Go Client
permalink: /develop/avatica-go.html
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

* TOC
{:toc}

## Issues

To file issues, please use the [Calcite JIRA](https://issues.apache.org/jira/projects/CALCITE/issues) and select `avatica-go`
as the component.

## Updating protobuf definitions
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).
2. From the root of the repository, run `docker-compose run compile-protobuf`

## Live reload during development
It is possible to reload the code in real-time during development. This executes the test suite every time a `.go` or
`.mod` file is updated. The test suite takes a while to run, so the tests will not complete instantly, but live-reloading
during development allows us to not have to manually execute the test suite on save.

### Set up
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).

2. From the root of the repository, run `docker-compose run dev`.

3. After terminating the container, stop all the containers and remove them using: `docker-compose down`

## Testing
The test suite takes around 4 minutes to run if you run both the Avatica HSQLDB and Apache Phoenix tests.

### Easy way
1. Install [docker](https://docs.docker.com/install/) and [docker-compose](https://docs.docker.com/compose/install/).

2. From the root of the repository, run `docker-compose run test`.

3. After the tests complete, stop all the containers and remove them using: `docker-compose down`

### Manual set up
1. Install [Go](https://golang.org/doc/install).

2. The test suite requires access to an instance of Avatica running HSQLDB and an instance of Apache Phoenix running the
Phoenix Query Server.

You should then set the `HSQLDB_HOST` and `PHOENIX_HOST` environment variables. For example:
{% highlight shell %}
HSQLDB_HOST: http://hsqldb:8765
PHOENIX_HOST: http://phoenix:8765
{% endhighlight %}

3. To select the test suite, export `AVATICA_FLAVOR=HSQLDB` for Avatica HSQLDB or `AVATICA_FLAVOR=PHOENIX` for Phoenix.

4. Then run `go test -v ./...` from the root of the repository to execute the test suite.