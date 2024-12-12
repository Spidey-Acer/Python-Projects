
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
# Apache Hadoop Changelog

## Release 3.4.1 - 2024-09-23



### NEW FEATURES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-19131](https://issues.apache.org/jira/browse/HADOOP-19131) | WrappedIO to export modern filesystem/statistics APIs in a reflection friendly form |  Major | fs, fs/azure, fs/s3 | Steve Loughran | Steve Loughran |


### IMPROVEMENTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17359](https://issues.apache.org/jira/browse/HDFS-17359) | EC: recheck failed streamers should only after flushing all packets. |  Minor | ec | farmmamba | farmmamba |
| [HADOOP-18987](https://issues.apache.org/jira/browse/HADOOP-18987) | Corrections to Hadoop FileSystem API Definition |  Minor | documentation | Dieter De Paepe | Dieter De Paepe |
| [HADOOP-18993](https://issues.apache.org/jira/browse/HADOOP-18993) | S3A: Add option fs.s3a.classloader.isolation (#6301) |  Minor | fs/s3 | Antonio Murgia | Antonio Murgia |
| [HADOOP-19059](https://issues.apache.org/jira/browse/HADOOP-19059) | S3A: update AWS SDK to 2.23.19 to support S3 Access Grants |  Minor | build, fs/s3 | Jason Han | Jason Han |
| [HADOOP-19065](https://issues.apache.org/jira/browse/HADOOP-19065) | Update Protocol Buffers installation to 3.21.12 |  Major | build | Zhaobo Huang | Zhaobo Huang |
| [HADOOP-19082](https://issues.apache.org/jira/browse/HADOOP-19082) | S3A: Update AWS SDK V2 to 2.24.6 |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [YARN-11657](https://issues.apache.org/jira/browse/YARN-11657) | Remove protobuf-2.5 as dependency of hadoop-yarn-api |  Major | api | Steve Loughran | Steve Loughran |
| [HDFS-17404](https://issues.apache.org/jira/browse/HDFS-17404) | Add Namenode info to log message when setting block keys from active nn |  Trivial | . | Joseph Dell'Aringa |  |
| [HADOOP-19090](https://issues.apache.org/jira/browse/HADOOP-19090) | Update Protocol Buffers installation to 3.23.4 |  Major | build | PJ Fanning | PJ Fanning |
| [HDFS-17431](https://issues.apache.org/jira/browse/HDFS-17431) | Fix log format for BlockRecoveryWorker#recoverBlocks |  Major | . | Haiyang Hu | Haiyang Hu |
| [MAPREDUCE-7469](https://issues.apache.org/jira/browse/MAPREDUCE-7469) | NNBench createControlFiles should use thread pool to improve performance. |  Minor | mapreduce-client | liuguanghua |  |
| [HADOOP-19052](https://issues.apache.org/jira/browse/HADOOP-19052) | Hadoop use Shell command to get the count of the hard link which takes a lot of time |  Major | fs | liang yu |  |
| [HADOOP-19047](https://issues.apache.org/jira/browse/HADOOP-19047) | Support InMemory Tracking Of S3A Magic Commits |  Major | fs/s3 | Syed Shameerur Rahman | Syed Shameerur Rahman |
| [HDFS-17429](https://issues.apache.org/jira/browse/HDFS-17429) | Datatransfer sender.java LOG variable uses interface's, causing log fileName mistake |  Trivial | . | Zhongkun Wu |  |
| [YARN-11663](https://issues.apache.org/jira/browse/YARN-11663) | [Federation] Add Cache Entity Nums Limit. |  Major | federation, yarn | Yuan Luo | Shilun Fan |
| [HADOOP-19135](https://issues.apache.org/jira/browse/HADOOP-19135) | Remove Jcache 1.0-alpha |  Major | common | Shilun Fan | Shilun Fan |
| [YARN-11444](https://issues.apache.org/jira/browse/YARN-11444) | Improve YARN md documentation format |  Major | yarn | Shilun Fan | Shilun Fan |
| [HDFS-17367](https://issues.apache.org/jira/browse/HDFS-17367) | Add PercentUsed for Different StorageTypes in JMX |  Major | metrics, namenode | Hualong Zhang | Hualong Zhang |
| [HADOOP-19159](https://issues.apache.org/jira/browse/HADOOP-19159) | Fix hadoop-aws document for fs.s3a.committer.abort.pending.uploads |  Minor | documentation | Xi Chen | Xi Chen |
| [HADOOP-19146](https://issues.apache.org/jira/browse/HADOOP-19146) | noaa-cors-pds bucket access with global endpoint fails |  Minor | fs/s3, test | Viraj Jasani | Viraj Jasani |
| [HADOOP-19160](https://issues.apache.org/jira/browse/HADOOP-19160) | hadoop-auth should not depend on kerb-simplekdc |  Major | auth | Attila Doroszlai | Attila Doroszlai |
| [HADOOP-19172](https://issues.apache.org/jira/browse/HADOOP-19172) | Upgrade aws-java-sdk to 1.12.720 |  Minor | build, fs/s3 | Steve Loughran | Steve Loughran |
| [YARN-11471](https://issues.apache.org/jira/browse/YARN-11471) | FederationStateStoreFacade Cache Support Caffeine |  Major | federation | Shilun Fan | Shilun Fan |
| [YARN-11699](https://issues.apache.org/jira/browse/YARN-11699) | Diagnostics lacks userlimit info when user capacity has reached its maximum limit |  Major | capacity scheduler | Jiandan Yang | Jiandan Yang |
| [HADOOP-19192](https://issues.apache.org/jira/browse/HADOOP-19192) | Log level is WARN when fail to load native hadoop libs |  Minor | documentation | Cheng Pan | Cheng Pan |
| [HADOOP-18931](https://issues.apache.org/jira/browse/HADOOP-18931) | FileSystem.getFileSystemClass() to log at debug the jar the .class came from |  Minor | fs | Steve Loughran | Viraj Jasani |
| [HADOOP-19203](https://issues.apache.org/jira/browse/HADOOP-19203) | WrappedIO BulkDelete API to raise IOEs as UncheckedIOExceptions |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-19194](https://issues.apache.org/jira/browse/HADOOP-19194) | Add test to find unshaded dependencies in the aws sdk |  Major | fs/s3 | Harshit Gupta | Harshit Gupta |
| [HADOOP-19218](https://issues.apache.org/jira/browse/HADOOP-19218) | Avoid DNS lookup while creating IPC Connection object |  Major | ipc | Viraj Jasani | Viraj Jasani |
| [HADOOP-19161](https://issues.apache.org/jira/browse/HADOOP-19161) | S3A: option "fs.s3a.performance.flags" to take list of performance flags |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19244](https://issues.apache.org/jira/browse/HADOOP-19244) | Pullout arch-agnostic maven javadoc plugin configurations in hadoop-common |  Major | build, common | Cheng Pan | Cheng Pan |
| [HADOOP-17609](https://issues.apache.org/jira/browse/HADOOP-17609) | Make SM4 support optional for OpenSSL native code |  Major | native | Masatake Iwasaki | Masatake Iwasaki |
| [HADOOP-19136](https://issues.apache.org/jira/browse/HADOOP-19136) | Upgrade commons-io to 2.16.1 |  Major | common | Shilun Fan | Shilun Fan |
| [HADOOP-19249](https://issues.apache.org/jira/browse/HADOOP-19249) | Getting NullPointerException when the unauthorised user tries to perform the key operation |  Major | common, security | Dhaval Shah |  |
| [HADOOP-18487](https://issues.apache.org/jira/browse/HADOOP-18487) | Make protobuf 2.5 an optional runtime dependency. |  Major | build, ipc | Steve Loughran | Steve Loughran |


### BUG FIXES:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-19049](https://issues.apache.org/jira/browse/HADOOP-19049) | Class loader leak caused by StatisticsDataReferenceCleaner thread |  Major | common | Jia Fan | Jia Fan |
| [HDFS-17299](https://issues.apache.org/jira/browse/HDFS-17299) | HDFS is not rack failure tolerant while creating a new file. |  Critical | . | Rushabh Shah | Ritesh |
| [YARN-11660](https://issues.apache.org/jira/browse/YARN-11660) | SingleConstraintAppPlacementAllocator performance regression |  Major | scheduler | Junfan Zhang | Junfan Zhang |
| [HDFS-17354](https://issues.apache.org/jira/browse/HDFS-17354) | Delay invoke  clearStaleNamespacesInRouterStateIdContext during router start up |  Major | . | lei w | lei w |
| [HADOOP-19116](https://issues.apache.org/jira/browse/HADOOP-19116) | update to zookeeper client 3.8.4 due to  CVE-2024-23944 |  Major | CVE | PJ Fanning | PJ Fanning |
| [YARN-11668](https://issues.apache.org/jira/browse/YARN-11668) | Potential concurrent modification exception for node attributes of node manager |  Major | . | Junfan Zhang | Junfan Zhang |
| [HADOOP-19115](https://issues.apache.org/jira/browse/HADOOP-19115) | upgrade to nimbus-jose-jwt 9.37.2 due to CVE |  Major | build, CVE | PJ Fanning | PJ Fanning |
| [HADOOP-19110](https://issues.apache.org/jira/browse/HADOOP-19110) | ITestExponentialRetryPolicy failing in branch-3.4 |  Major | fs/azure | Mukund Thakur | Anuj Modi |
| [YARN-11684](https://issues.apache.org/jira/browse/YARN-11684) | PriorityQueueComparator violates general contract |  Major | capacityscheduler | Tamas Domok | Tamas Domok |
| [HADOOP-19170](https://issues.apache.org/jira/browse/HADOOP-19170) | Fixes compilation issues on Mac |  Major | . | Chenyu Zheng | Chenyu Zheng |
| [HDFS-17520](https://issues.apache.org/jira/browse/HDFS-17520) | TestDFSAdmin.testAllDatanodesReconfig and TestDFSAdmin.testDecommissionDataNodesReconfig failed |  Major | hdfs | ZanderXu | ZanderXu |
| [MAPREDUCE-7474](https://issues.apache.org/jira/browse/MAPREDUCE-7474) | [ABFS] Improve commit resilience and performance in Manifest Committer |  Major | client | Steve Loughran | Steve Loughran |
| [MAPREDUCE-7475](https://issues.apache.org/jira/browse/MAPREDUCE-7475) | Fix non-idempotent unit tests |  Minor | test | Kaiyao Ke | Kaiyao Ke |
| [HADOOP-18962](https://issues.apache.org/jira/browse/HADOOP-18962) | Upgrade kafka to 3.4.0 |  Major | build | D M Murali Krishna Reddy | D M Murali Krishna Reddy |
| [HADOOP-19188](https://issues.apache.org/jira/browse/HADOOP-19188) | TestHarFileSystem and TestFilterFileSystem failing after bulk delete API added |  Minor | fs, test | Steve Loughran | Mukund Thakur |
| [HADOOP-19114](https://issues.apache.org/jira/browse/HADOOP-19114) | upgrade to commons-compress 1.26.1 due to cves |  Major | build, CVE | PJ Fanning | PJ Fanning |
| [HADOOP-19196](https://issues.apache.org/jira/browse/HADOOP-19196) | Bulk delete api doesn't take the path to delete as the base path |  Minor | fs | Steve Loughran | Mukund Thakur |
| [YARN-11701](https://issues.apache.org/jira/browse/YARN-11701) | Enhance Federation Cache Clean Conditions |  Major | federation | Shilun Fan | Shilun Fan |
| [HADOOP-19222](https://issues.apache.org/jira/browse/HADOOP-19222) | Switch yum repo baseurl due to CentOS 7 sunset |  Major | build | Cheng Pan | Cheng Pan |
| [HADOOP-19238](https://issues.apache.org/jira/browse/HADOOP-19238) | Fix create-release script for arm64 based MacOS |  Major | . | Mukund Thakur | Mukund Thakur |
| [HADOOP-19153](https://issues.apache.org/jira/browse/HADOOP-19153) | hadoop-common still exports logback as a transitive dependency |  Major | build, common | Steve Loughran | Steve Loughran |
| [HADOOP-18542](https://issues.apache.org/jira/browse/HADOOP-18542) | Azure Token provider requires tenant and client IDs despite being optional |  Major | fs/azure, hadoop-thirdparty | Carl |  |
| [HADOOP-19271](https://issues.apache.org/jira/browse/HADOOP-19271) | [ABFS]: NPE in AbfsManagedApacheHttpConnection.toString() when not connected |  Blocker | fs/azure | Steve Loughran | Pranav Saxena |
| [HADOOP-19285](https://issues.apache.org/jira/browse/HADOOP-19285) | [ABFS] Restore ETAGS\_AVAILABLE to abfs path capabilities |  Critical | fs/azure | Steve Loughran | Steve Loughran |


### TESTS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17370](https://issues.apache.org/jira/browse/HDFS-17370) | Fix junit dependency for running parameterized tests in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17432](https://issues.apache.org/jira/browse/HDFS-17432) | Fix junit dependency to enable JUnit4 tests to run in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17435](https://issues.apache.org/jira/browse/HDFS-17435) | Fix TestRouterRpc failed |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HDFS-17441](https://issues.apache.org/jira/browse/HDFS-17441) | Fix junit dependency by adding missing library in hadoop-hdfs-rbf |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-19190](https://issues.apache.org/jira/browse/HADOOP-19190) | Skip ITestS3AEncryptionWithDefaultS3Settings.testEncryptionFileAttributes when bucket not encrypted with sse-kms |  Minor | fs/s3 | Mukund Thakur | Mukund Thakur |


### SUB-TASKS:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HADOOP-19004](https://issues.apache.org/jira/browse/HADOOP-19004) | S3A: Support Authentication through HttpSigner API |  Major | fs/s3 | Steve Loughran | Harshit Gupta |
| [HADOOP-19027](https://issues.apache.org/jira/browse/HADOOP-19027) | S3A: S3AInputStream doesn't recover from HTTP/channel exceptions |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18883](https://issues.apache.org/jira/browse/HADOOP-18883) | Expect-100 JDK bug resolution: prevent multiple server calls |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19015](https://issues.apache.org/jira/browse/HADOOP-19015) | Increase fs.s3a.connection.maximum to 500 to minimize risk of Timeout waiting for connection from pool |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18975](https://issues.apache.org/jira/browse/HADOOP-18975) | AWS SDK v2:  extend support for FIPS endpoints |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19046](https://issues.apache.org/jira/browse/HADOOP-19046) | S3A: update AWS sdk versions to 2.23.5 and 1.12.599 |  Major | build, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18830](https://issues.apache.org/jira/browse/HADOOP-18830) | S3A: Cut S3 Select |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-18980](https://issues.apache.org/jira/browse/HADOOP-18980) | S3A credential provider remapping: make extensible |  Minor | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-19044](https://issues.apache.org/jira/browse/HADOOP-19044) | AWS SDK V2 - Update S3A region logic |  Major | fs/s3 | Ahmar Suhail | Viraj Jasani |
| [HADOOP-19045](https://issues.apache.org/jira/browse/HADOOP-19045) | HADOOP-19045. S3A: CreateSession Timeout after 10 seconds |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19057](https://issues.apache.org/jira/browse/HADOOP-19057) | S3 public test bucket landsat-pds unreadable -needs replacement |  Critical | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19084](https://issues.apache.org/jira/browse/HADOOP-19084) | prune dependency exports of hadoop-\* modules |  Blocker | build | Steve Loughran | Steve Loughran |
| [HADOOP-19097](https://issues.apache.org/jira/browse/HADOOP-19097) | core-default fs.s3a.connection.establish.timeout value too low -warning always printed |  Minor | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19066](https://issues.apache.org/jira/browse/HADOOP-19066) | AWS SDK V2 - Enabling FIPS should be allowed with central endpoint |  Major | fs/s3 | Viraj Jasani | Viraj Jasani |
| [HADOOP-19119](https://issues.apache.org/jira/browse/HADOOP-19119) | spotbugs complaining about possible NPE in org.apache.hadoop.crypto.key.kms.ValueQueue.getSize() |  Minor | crypto | Steve Loughran | Steve Loughran |
| [HADOOP-19089](https://issues.apache.org/jira/browse/HADOOP-19089) | [ABFS] Reverting Back Support of setXAttr() and getXAttr() on root path |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19141](https://issues.apache.org/jira/browse/HADOOP-19141) | Update VectorIO default values consistently |  Major | fs, fs/s3 | Dongjoon Hyun | Dongjoon Hyun |
| [HADOOP-19098](https://issues.apache.org/jira/browse/HADOOP-19098) | Vector IO: consistent specified rejection of overlapping ranges |  Major | fs, fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19101](https://issues.apache.org/jira/browse/HADOOP-19101) | Vectored Read into off-heap buffer broken in fallback implementation |  Blocker | fs, fs/azure | Steve Loughran | Steve Loughran |
| [HADOOP-19096](https://issues.apache.org/jira/browse/HADOOP-19096) | [ABFS] Enhancing Client-Side Throttling Metrics Updation Logic |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19129](https://issues.apache.org/jira/browse/HADOOP-19129) | ABFS: Fixing Test Script Bug and Some Known test Failures in ABFS Test Suite |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19102](https://issues.apache.org/jira/browse/HADOOP-19102) | [ABFS]: FooterReadBufferSize should not be greater than readBufferSize |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19150](https://issues.apache.org/jira/browse/HADOOP-19150) | Test ITestAbfsRestOperationException#testAuthFailException is broken. |  Major | . | Mukund Thakur | Anuj Modi |
| [HADOOP-19013](https://issues.apache.org/jira/browse/HADOOP-19013) | fs.getXattrs(path) for S3FS doesn't have x-amz-server-side-encryption-aws-kms-key-id header. |  Major | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18011](https://issues.apache.org/jira/browse/HADOOP-18011) | ABFS: Enable config control for default connection timeout |  Major | fs/azure | Sneha Vijayarajan | Sneha Vijayarajan |
| [HADOOP-18759](https://issues.apache.org/jira/browse/HADOOP-18759) | [ABFS][Backoff-Optimization] Have a Static retry policy for connection timeout failures |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19184](https://issues.apache.org/jira/browse/HADOOP-19184) | TestStagingCommitter.testJobCommitFailure failing |  Critical | fs/s3 | Mukund Thakur | Mukund Thakur |
| [HADOOP-18679](https://issues.apache.org/jira/browse/HADOOP-18679) | Add API for bulk/paged delete of files and objects |  Major | fs/s3 | Steve Loughran | Mukund Thakur |
| [HADOOP-19178](https://issues.apache.org/jira/browse/HADOOP-19178) | WASB Driver Deprecation and eventual removal |  Major | fs/azure | Sneha Vijayarajan | Anuj Modi |
| [HADOOP-18516](https://issues.apache.org/jira/browse/HADOOP-18516) | [ABFS]: Support fixed SAS token config in addition to Custom SASTokenProvider Implementation |  Minor | fs/azure | Sree Bhattacharyya | Anuj Modi |
| [HADOOP-19137](https://issues.apache.org/jira/browse/HADOOP-19137) | [ABFS]Prevent ABFS initialization for non-hierarchical-namespace account if Customer-provided-key configs given. |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-18508](https://issues.apache.org/jira/browse/HADOOP-18508) | support multiple s3a integration test runs on same bucket in parallel |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19204](https://issues.apache.org/jira/browse/HADOOP-19204) | VectorIO regression: empty ranges are now rejected |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-19210](https://issues.apache.org/jira/browse/HADOOP-19210) | s3a: TestS3AAWSCredentialsProvider and TestS3AInputStreamRetry really slow |  Major | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19205](https://issues.apache.org/jira/browse/HADOOP-19205) | S3A initialization/close slower than with v1 SDK |  Major | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19208](https://issues.apache.org/jira/browse/HADOOP-19208) | ABFS: Fixing logic to determine HNS nature of account to avoid extra getAcl() calls |  Major | fs/azure | Anuj Modi | Anuj Modi |
| [HADOOP-19120](https://issues.apache.org/jira/browse/HADOOP-19120) | [ABFS]: ApacheHttpClient adaptation as network library |  Major | fs/azure | Pranav Saxena | Pranav Saxena |
| [HADOOP-19245](https://issues.apache.org/jira/browse/HADOOP-19245) | S3ABlockOutputStream no longer sends progress events in close() |  Critical | fs/s3 | Steve Loughran | Steve Loughran |
| [HADOOP-19253](https://issues.apache.org/jira/browse/HADOOP-19253) | Google GCS changes fail due to VectorIO changes |  Major | fs | Steve Loughran | Steve Loughran |
| [HADOOP-18965](https://issues.apache.org/jira/browse/HADOOP-18965) | ITestS3AHugeFilesEncryption failure |  Major | fs/s3, test | Steve Loughran |  |
| [HADOOP-19072](https://issues.apache.org/jira/browse/HADOOP-19072) | S3A: expand optimisations on stores with "fs.s3a.performance.flags" for mkdir |  Major | fs/s3 | Steve Loughran | Viraj Jasani |
| [HADOOP-19257](https://issues.apache.org/jira/browse/HADOOP-19257) | S3A: ITestAssumeRole.testAssumeRoleBadInnerAuth failure |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |
| [HADOOP-19201](https://issues.apache.org/jira/browse/HADOOP-19201) | S3A: Support external id in assume role |  Major | fs/s3 | Smith Cruise | Smith Cruise |
| [HADOOP-19189](https://issues.apache.org/jira/browse/HADOOP-19189) | ITestS3ACommitterFactory failing |  Minor | fs/s3, test | Steve Loughran | Steve Loughran |


### OTHER:

| JIRA | Summary | Priority | Component | Reporter | Contributor |
|:---- |:---- | :--- |:---- |:---- |:---- |
| [HDFS-17362](https://issues.apache.org/jira/browse/HDFS-17362) | RBF: Implement RouterObserverReadConfiguredFailoverProxyProvider |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-19024](https://issues.apache.org/jira/browse/HADOOP-19024) | Use bouncycastle jdk18 1.77 |  Major | . | PJ Fanning | PJ Fanning |
| [HDFS-17450](https://issues.apache.org/jira/browse/HDFS-17450) | Add explicit dependency on httpclient jar |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19123](https://issues.apache.org/jira/browse/HADOOP-19123) | Update commons-configuration2 to 2.10.1 due to CVE |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19079](https://issues.apache.org/jira/browse/HADOOP-19079) | HttpExceptionUtils to check that loaded class is really an exception before instantiation |  Major | common, security | PJ Fanning | PJ Fanning |
| [HDFS-17591](https://issues.apache.org/jira/browse/HDFS-17591) | RBF: Router should follow X-FRAME-OPTIONS protection setting |  Major | . | Takanobu Asanuma | Takanobu Asanuma |
| [HADOOP-19237](https://issues.apache.org/jira/browse/HADOOP-19237) | upgrade dnsjava to 3.6.1 due to CVEs |  Major | . | PJ Fanning | PJ Fanning |
| [HADOOP-19252](https://issues.apache.org/jira/browse/HADOOP-19252) | Release Hadoop Third-Party 1.3.0 |  Major | hadoop-thirdparty | Steve Loughran | Steve Loughran |


