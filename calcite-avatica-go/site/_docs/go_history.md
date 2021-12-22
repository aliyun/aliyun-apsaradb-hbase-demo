---
layout: docs
title: Go Client History
permalink: /docs/go_history.html
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

For a full list of releases, see
<a href="https://github.com/apache/calcite-avatica-go/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/avatica-go.html).

## <a href="https://github.com/apache/calcite-avatica-go/releases/tag/v5.0.0">5.0.0</a> / 2020-07-16
{: #v5-0-0}

Apache Calcite Avatica Go 5.0.0 is a major release of Avatica Go with a number of improvements and a breaking change.
As Go modules has been available since Go 1.11 (3 versions back as of writing), users of this library should
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

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-3248">CALCITE-3248</a>]
  Add Connector implementation and remove `username` and `password` query string parameters from DSN (Tino Rusch)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3253">CALCITE-3253</a>]
  Check server address exists before returning it in an Avatica response error (Tino Rusch)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3264">CALCITE-3264</a>]
  Add catch-all type for unknown types in all adapters instead of panicking (Tino Rusch)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3275">CALCITE-3275</a>]
  Add nil checks to error parsing (Tino Rusch)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2499">CALCITE-2499</a>]
  Drop support for dep
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3320">CALCITE-3320</a>]
  Use transitional x/xerrors package when working with errors
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3903">CALCITE-3903</a>]
  Upgrade protobuf generation dependencies and regenerate avatica protobufs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3905">CALCITE-3905</a>]
  Upgrade gokrb5 to v8
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3906">CALCITE-3906</a>]
  Pin witch version in tools.go file
* [<a href="https://issues.apache.org/jira/browse/CALCITE-4067">CALCITE-4067</a>]
  Add support for ExecuteBatchRequest in prepared statement (chenhualin)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3359">CALCITE-3359</a>]
  Update dependencies
* [<a href="https://issues.apache.org/jira/browse/CALCITE-4078">CALCITE-4078</a>]
  Move import path from v4 to v5 for 5.0.0 release
  
Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-3356">CALCITE-3356</a>]
  Use Github Actions for continuous integration
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3902">CALCITE-3902</a>]
  Upgrade Go to 1.13 and 1.14
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3904">CALCITE-3904</a>]
  Upgrade Github Actions dependencies
* [<a href="https://issues.apache.org/jira/browse/CALCITE-4076">CALCITE-4076</a>]
  Test against Avatica 1.17.0 and regenerate protobuf

Web site and documentation:

* Clean up documentation and remove references to dep
 
## <a href="https://github.com/apache/calcite-avatica-go/releases/tag/v4.0.0">4.0.0</a> / 2019-05-16
{: #v4-0-0}

Apache Calcite Avatica Go 4.0.0 is a major release of Avatica Go with a number of improvements and a breaking change.
This release supports using both [dep](https://github.com/golang/dep) and Go modules for package management. As Go modules
will be turned on by default in Go 1.13.0 (estimated to be released in September/October 2019), it is highly recommended
that users of this package start migrating to using Go modules to ease the transition.

**Breaking change for Phoenix ([CALCITE-2763](https://issues.apache.org/jira/browse/CALCITE-2724)):** 
In Apache Phoenix, null and empty strings are equivalent. For some background on why this is the case, see
[PHOENIX-947](https://issues.apache.org/jira/browse/PHOENIX-947). In version 3 of Avatica-Go and below, null and empty
strings are returned as an empty string `""` to the client. This prevented database/sql's built in NullString type from
working correctly. From 4.0.0 onwards, null and empty strings will be returned as a `nil`. This allows the usage of the
`sql.NullString` type.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2723">CALCITE-2723</a>]
  Generate SHA512 digest for releases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2724">CALCITE-2724</a>]
  Exclude .md files from import path check in release script to avoid false positives
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2579">CALCITE-2579</a>]
  Implement live reloading of tests when source files change during development
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2763">CALCITE-2763</a>]
  Fix handling of nils (nulls) when executing queries and scanning query results with empty strings and other null types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2775">CALCITE-2775</a>]
  Update dependencies and regenerate protobufs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3024">CALCITE-3024</a>]
  Update dependencies (April 26 2019)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3032">CALCITE-3032</a>]
  Simplify docker-compose.yml for running tests and development mode, change release process to use a docker container
  to build in a clean environment and include automation for uploading and promoting releases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3026">CALCITE-3026</a>]
  Move import paths from v3 to v4 to prepare for 4.0.0 release
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3042">CALCITE-3042</a>]
  Fix bug in docker release script that prevents releases from being promoted correctly and incorrect variable
  substitution in vote email

Tests

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2771">CALCITE-2771</a>]
  Test against Avatica HSQLDB 1.13.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3025">CALCITE-3025</a>]
  Update travis configuration and docker-compose to test against Go 1.12 and run tests using dep in Go 1.11 and Go 1.12
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3035">CALCITE-3035</a>]
  Test against Avatica HSQLDB 1.14.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3044">CALCITE-3044</a>]
  Test against Avatica HSQLDB 1.15.0 and simplify Alpine dependencies

Web site and documentation:

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2774">CALCITE-2774</a>]
  Improve release documentation and explicitly include all steps for making a release
* [<a href="https://issues.apache.org/jira/browse/CALCITE-3033">CALCITE-3033</a>]
  Move release process to separate HOWTO document so that it's consistent with how the avatica docs are structured
 
## <a href="https://github.com/apache/calcite-avatica-go/releases/tag/v3.2.0">3.2.0</a> / 2018-09-18
{: #v3-2-0}

Apache Calcite Avatica Go 3.2.0 is a minor release of Avatica Go with fixes to the import paths after enabling
support for Go modules.

The 3.1.0 release contained a bug where packages within the library used the `"github.com/apache/calcite-avatica-go"`
import path rather than the `"github.com/apache/calcite-avatica-go/v3"` import path. This resulted in an issue where
2 versions of the library are being used at the same time, causing some programs to not build.

**The Calcite team recommends consumers of the Avatica Go library to not use the 3.1.0 release and ensure that the
3.2.0 release is being used.**

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2536">CALCITE-2536</a>]
  Update release script to check that import paths within the library point to the correct version
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2545">CALCITE-2545</a>]
  Fix incorrect import paths within the library to point to the correct version: github.com/apache/calcite-avatica-go/v3
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2544">CALCITE-2544</a>]
  Replace the golang.org/x/net/context package with the context package in the standard library
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2547">CALCITE-2547</a>]
  Update all dependencies to their latest versions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2550">CALCITE-2550</a>]
  Update release script to build tarballs with filenames in the format: apache-calcite-avatica-go-x.x.x-src.tar.gz

## <a href="https://github.com/apache/calcite-avatica-go/releases/tag/v3.1.0">3.1.0</a> / 2018-09-10
{: #v3-1-0}

Apache Calcite Avatica Go 3.1.0 is a minor release of Avatica Go with support for Go modules.
We recommend using the latest stable version of Go.

Go 1.11 along with Go modules support was released at the end of August 2018. Go modules will become the
official package management solution for Go projects. As the Go team currently supports both Go 1.11 and Go 1.10,
the Gopkg.toml and Gopkg.lock files are still available for those using dep for package management. We plan to
remove support for dep when Go 1.12 is released in early 2019, so we encourage users to upgrade to Go 1.11 and use
Go modules where possible.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2333">CALCITE-2333</a>]
  Stop releasing zip archives
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2367">CALCITE-2367</a>]
  Remove the satori/go.uuid package as it is unmaintained and replace it with hashicorp/go-uuid which is already
  a transitive dependency (Kenneth Shaw)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2372">CALCITE-2372</a>]
  Test against Apache Phoenix 4.14.0
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2493">CALCITE-2493</a>]
  Update all dependencies to their latest versions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2335">CALCITE-2335</a>]
  Add support for Go modules (available since Go 1.11) and test against Go 1.11
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2500">CALCITE-2500</a>]
  Test against Avatica 1.12.0 and Apache Phoenix 5.0.0 and regenerate protobuf definitions
* Fix release script
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2513">CALCITE-2513</a>]
  Fix dockerfile so that Go modules work correctly when running tests using docker-compose
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2513">CALCITE-2531</a>]
  Update release script to only include files in source control
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2533">CALCITE-2533</a>]
  Allow user to select signing key when signing releases using release script
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2532">CALCITE-2532</a>]
  Update release script to check ending copyright year in NOTICE file
* [<a href="https://issues.apache.org/jira/browse/CALCITE-2534">CALCITE-2534</a>]
  Update release script to check that AVATICA_VER are the same in both gen-protobuf.sh and gen-protobuf.bat
* Fix 3.1.0 release annoucement filename

Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-2335">CALCITE-2335</a>]
  Update documentation and release instructions to include support for Go modules.

## <a href="https://github.com/apache/calcite-avatica-go/releases/tag/3.0.0">3.0.0</a> / 2018-04-27
{: #v3-0-0}

Apache Calcite Avatica Go 3.0.0 is the first release since the Go driver has been donated
to the Apache Software foundation.
We recommend using the latest stable version of Go.

Features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1240">CALCITE-1240</a>]
  Intial import of the original [Boostport/avatica](https://github.com/Boostport/avatica) code-base into the
  [apache/calcite-avatica-go](https://github.com/apache/calcite-avatica-go) repository
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1938">CALCITE-1938</a>]
  Releasing the first release of Calcite Avatica Go under the Apache Software Foundation
* Remove go-cleanhttp dependency
* Support for Avatica HSQLDB backend and move Apache Phoenix support into adapter
* Add bash script to automate releases with checks to alert on files without the Apache license header
* Replace gopher.png test fixture with Calcite logo

Web site and documentation

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1937">CALCITE-1937</a>]
  Set up Calcite Avatica Go website

## Past releases

Prior to release 3.0.0, the Avatica Go client was developed by Boostport.

Please refer to the [Boostport/avatica](https://github.com/Boostport/avatica) Github repository for previous releases
of the Avatica Go client.