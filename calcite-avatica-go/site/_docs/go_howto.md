---
layout: docs
title: HOWTO
permalink: /docs/go_howto.html
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

Here's some miscellaneous documentation about using Avatica.

* TOC
{:toc}

## Releasing

### Preparing for release
1. You will need to have [docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.

2. If you have not set up a GPG signing key, set one up by following these [instructions](https://www.apache.org/dev/openpgp.html#generate-key).

3. If this release is a new major version (we are releasing 4.0.0 vs the current version 3.0.0), update the version in the
import path in `go.mod`. The import paths in the various sample code snippets should also be updated.

4. Since we need to support Go modules, tags must be prefixed with a `v`. For example, tag as `v3.1.0` rather than `3.1.0`.

5. Check that `NOTICE` has the current copyright year.

### Perform a dry-run
* The script expects you to mount your `~/.gnupg` directory into the `/.gnupg` directory in the container. Once mounted into the container,
the script will make a copy of the contents and move it to a different location, so that it will not modify the contents of your original
`~/.gnupg` directory during the build.

{% highlight bash %}
# On Linux:
docker-compose run -v ~/.gnupg:/.gnupg dry-run

# On Windows
docker-compose run -v /c/Users/username/AppData/Roaming/gnupg:/.gnupg dry-run
{% endhighlight %}

### Build the release
{% highlight bash %}
# On Linux:
docker-compose run -v ~/.gnupg:/.gnupg release

# On Windows
docker-compose run -v /c/Users/username/AppData/Roaming/gnupg:/.gnupg release
{% endhighlight %}

If the build fails, perform a clean:
1. Remove the git tag locally and remotely:
{% highlight bash %}
git tag -d vX.Y.Z-rcA
git push origin :refs/tags/vX.Y.Z-rcA
{% endhighlight %}

2. Clean the local repository
{% highlight bash %}
docker-compose run clean
{% endhighlight %}

### Check the release before uploading
The name of the release folder must be in the following format: `apache-calcite-avatica-go-X.Y.Z-rcN`. The version must 
include release candidate identifiers such as `-rc0`, if they are present.

The files inside the release folder must have any release candidate identifiers such as `-rc1` removed, even if the
release is a release candidate. `src` must also be added to the filename.

For example, if we are uploading the `apache-calcite-avatica-go-3.0.0-rc1` folder, the files must be named 
`apache-calcite-avatica-go-3.0.0-src.tar.gz`. Note the inclusion of `src` in the filename.

The tar.gz must be named `apache-calcite-avatica-go-X.Y.Z-src.tar.gz`. 

There must be a GPG signature for the tar.gz named: `apache-calcite-avatica-go-X.Y.Z-src.tar.gz.asc`

There must be a SHA512 hash for the tar.gz named: `apache-calcite-avatica-go-X.Y.Z-src.tar.gz.sha512`

### Uploading release artifacts to dev for voting
#### Manually
`svn` must be installed in order to upload release artifacts.

1. Check out the Calcite dev release subdirectory: `svn co "https://dist.apache.org/repos/dist/dev/calcite/" calcite-dev`.

2. Move the release folder under `dist/` into the `calcite-dev` folder.

3. Add the new release to the svn repository: `svn add apache-calcite-avatica-go-X.Y.Z-rcN`. Remember to change the folder name to the
correct release in the command.

4. Commit to upload the artifacts: `svn commit -m "apache-calcite-avatica-go-X.Y.Z-rcN" --username yourapacheusername --force-log`
Note the use of `--force-log` to suppress the svn warning, because the commit message is the same as the name of the directory.

#### Using docker
This assumes that a release was built and the artifacts are in the `dist/` folder.

{% highlight bash %}
docker-compose run publish-release-for-voting
{% endhighlight %}

The script will also generate a vote email to send to the dev list. You can use this email, but make sure to check that
all the details are correct.

### Send an email to the Dev list for voting:

Send out the email for voting:
{% highlight text %} 
To: dev@calcite.apache.org
Subject: [VOTE] Release apache-calcite-avatica-go-X.Y.Z (release candidate N)

Hi all,

I have created a build for Apache Calcite Avatica Go X.Y.Z, release candidate N.

Thanks to everyone who has contributed to this release. The release notes are available here:
https://github.com/apache/calcite-avatica-go/blob/XXXX/site/_docs/go_history.md

The commit to be voted upon:
https://gitbox.apache.org/repos/asf?p=calcite-avatica-go.git;a=commit;h=NNNNNN

The hash is XXXX.

The artifacts to be voted on are located here:
https://dist.apache.org/repos/dist/dev/calcite/apache-calcite-avatica-go-X.Y.Z-rcN/

The hashes of the artifacts are as follows:
src.tar.gz.sha512 XXXX

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/francischuang.asc

Instructions for running the test suite is located here:
https://github.com/apache/calcite-avatica-go/blob/$COMMIT/site/develop/avatica-go.md#testing

Please vote on releasing this package as Apache Calcite Avatica Go X.Y.Z.

To run the tests without a Go environment, install docker and docker-compose. Then, in the root of the release's directory, run:
docker-compose run test

When the test suite completes, run \"docker-compose down\" to remove and shutdown all the containers.

The vote is open for the next 72 hours and passes if a majority of
at least three +1 PMC votes are cast.

[ ] +1 Release this package as Apache Calcite Go X.Y.Z
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)

Francis
{% endhighlight %}

After vote finishes, send out the result:
{% highlight text %} 
Subject: [RESULT] [VOTE] Release apache-calcite-avatica-go-X.Y.Z (release candidate N)
To: dev@calcite.apache.org

Thanks to everyone who has tested the release candidate and given
their comments and votes.

The tally is as follows.

N binding +1s:
<names>

N non-binding +1s:
<names>

No 0s or -1s.

Therefore I am delighted to announce that the proposal to release
Apache Calcite Avatica Go X.Y.Z has passed.

Thanks everyone. We’ll now roll the release out to the mirrors.

Francis
{% endhighlight %}

### Promoting a release after voting
#### Manually
`svn` must be installed in order to upload release artifacts.

NOTE: Only official releases that has passed a vote may be uploaded to the release directory.

1. Check out the Calcite release directory: `svn co "https://dist.apache.org/repos/dist/release/calcite/" calcite-release`.

2. Copy the release into the `calcite-release` folder. Remember to check the name of the release's folder to ensure that it is in
the correct format.

3. Add the release to the svn repository: `svn add apache-calcite-avatica-go-X.Y.Z`. Remember to change the folder name to the
correct release in the command.

4. Commit to upload the artifacts: `svn commit -m "Release apache-calcite-avatica-go-X.Y.Z" --username yourapacheusername`.

5. Tag the final release in git and push it:

{% highlight bash %}
git tag vX.Y.Z X.Y.Z-rcN
git push origin vX.Y.Z
{% endhighlight %}

#### Using docker
This assumes that a rc release was tagged and pushed to the git repository.

{% highlight bash %}
docker-compose run promote-release
{% endhighlight %}

### Announce the release
After 24 hours, announce the release by sending an announcement to the [dev list](https://mail-archives.apache.org/mod_mbox/calcite-dev/)
and [announce@apache.org](https://mail-archives.apache.org/mod_mbox/www-announce/).

An example of the announcement could look like:
{% highlight text %}
Subject: [ANNOUNCE] Apache Calcite Avatica Go X.Y.Z released
To: dev@calcite.apache.org

The Apache Calcite team is pleased to announce the release of Apache Calcite Avatica Go X.Y.Z.

Avatica is a framework for building database drivers. Avatica
defines a wire API and serialization mechanism for clients to
communicate with a server as a proxy to a database. The reference
Avatica client and server are implemented in Java and communicate
over HTTP. Avatica is a sub-project of Apache Calcite.

The Avatica Go client is a Go database/sql driver that enables Go
programs to communicate with the Avatica server.

Apache Calcite Avatica Go X.Y.Z is a minor release of Avatica Go
with fixes to the import paths after enabling support for Go modules.

This release includes updated dependencies, testing against more
targets and support for Go Modules as described in the release notes:

    https://calcite.apache.org/avatica/docs/go_history.html#vX-Y-Z

The release is available here:

    https://calcite.apache.org/avatica/downloads/avatica-go.html

We welcome your help and feedback. For more information on how to
report problems, and to get involved, visit the project website at

    https://calcite.apache.org/avatica

Francis Chuang, on behalf of the Apache Calcite Team
{% endhighlight %}
