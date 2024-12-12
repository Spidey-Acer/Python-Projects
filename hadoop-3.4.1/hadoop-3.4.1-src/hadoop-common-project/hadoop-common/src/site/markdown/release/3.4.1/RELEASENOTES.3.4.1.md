
<!---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
-->
# Apache Hadoop  3.4.1 Release Notes

These release notes cover new developer and user-facing incompatibilities, important issues, features, and major improvements.


---

* [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | *Major* | **S3A: Cut S3 Select**

S3 Select is no longer supported through the S3A connector


---

* [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | *Minor* | **S3A: Add option fs.s3a.classloader.isolation (#6301)**

If the user wants to load custom implementations of AWS Credential Providers through user provided jars can set {{fs.s3a.extensions.isolated.classloader}} to {{false}}.


---

* [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | *Blocker* | **prune dependency exports of hadoop-\* modules**

maven/ivy imports of hadoop-common are less likely to end up with log4j versions on their classpath.


---

* [HADOOP-19101](https://issues.apache.org/jira/browse/HADOOP-19101) | *Blocker* | **Vectored Read into off-heap buffer broken in fallback implementation**

PositionedReadable.readVectored() will read incorrect data when reading from hdfs, azure abfs and other stores when given a direct buffer allocator. 

For cross-version compatibility, use on-heap buffer allocators only


---

* [HADOOP-19120](https://issues.apache.org/jira/browse/HADOOP-19120) | *Major* | **[ABFS]: ApacheHttpClient adaptation as network library**

Apache httpclient 4.5.x is a new implementation of http connections; this supports a large configurable pool of connections along with the ability to limit their lifespan.

The networking library can be chosen using the configuration
option fs.azure.networking.library

The supported values are
- JDK\_HTTP\_URL\_CONNECTION : Use JDK networking library  [Default]
- APACHE\_HTTP\_CLIENT : Use Apache HttpClient

Important: when the networking library is switched back to
the Apache http client, the apache httpcore and httpclient must be on the classpath.


---

* [HADOOP-18487](https://issues.apache.org/jira/browse/HADOOP-18487) | *Major* | **Make protobuf 2.5 an optional runtime dependency.**

hadoop modules no longer export protobuf-2.5.0 as a dependency, and it is omitted from the hadoop distribution directory. Applications which use the library must declare an explicit dependency.

Hadoop uses a shaded version of protobuf3 internally, and does not use the 2.5.0 JAR except when compiling compatible classes. It is still included in the binary distributions when the yarn timeline server is built with hbase 1



