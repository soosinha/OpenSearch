# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 3.0]
### Added
- Support for HTTP/2 (server-side) ([#3847](https://github.com/opensearch-project/OpenSearch/pull/3847))
- Add getter for path field in NestedQueryBuilder ([#4636](https://github.com/opensearch-project/OpenSearch/pull/4636))
- Allow mmap to use new JDK-19 preview APIs in Apache Lucene 9.4+ ([#5151](https://github.com/opensearch-project/OpenSearch/pull/5151))
- Add events correlation engine plugin ([#6854](https://github.com/opensearch-project/OpenSearch/issues/6854))
- Introduce new dynamic cluster setting to control slice computation for concurrent segment search ([#9107](https://github.com/opensearch-project/OpenSearch/pull/9107))
- Implement on behalf of token passing for extensions ([#8679](https://github.com/opensearch-project/OpenSearch/pull/8679))

### Dependencies
- Bump `log4j-core` from 2.18.0 to 2.19.0
- Bump `forbiddenapis` from 3.3 to 3.4
- Bump `avro` from 1.11.1 to 1.11.2
- Bump `woodstox-core` from 6.3.0 to 6.3.1
- Bump `xmlbeans` from 5.1.0 to 5.1.1 ([#4354](https://github.com/opensearch-project/OpenSearch/pull/4354))
- Bump `reactor-netty-core` from 1.0.19 to 1.0.22 ([#4447](https://github.com/opensearch-project/OpenSearch/pull/4447))
- Bump `reactive-streams` from 1.0.3 to 1.0.4 ([#4488](https://github.com/opensearch-project/OpenSearch/pull/4488))
- Bump `jempbox` from 1.8.16 to 1.8.17 ([#4550](https://github.com/opensearch-project/OpenSearch/pull/4550))
- Update to Gradle 7.6 and JDK-19 ([#4973](https://github.com/opensearch-project/OpenSearch/pull/4973))
- Update Apache Lucene to 9.5.0-snapshot-d5cef1c ([#5570](https://github.com/opensearch-project/OpenSearch/pull/5570))
- Bump `maven-model` from 3.6.2 to 3.8.6 ([#5599](https://github.com/opensearch-project/OpenSearch/pull/5599))
- Bump `maxmind-db` from 2.1.0 to 3.0.0 ([#5601](https://github.com/opensearch-project/OpenSearch/pull/5601))
- Bump `wiremock-jre8-standalone` from 2.33.2 to 2.35.0
- Bump `gson` from 2.10 to 2.10.1
- Bump `com.google.code.gson:gson` from 2.10 to 2.10.1
- Bump `com.maxmind.geoip2:geoip2` from 4.0.0 to 4.0.1
- Bump `com.avast.gradle:gradle-docker-compose-plugin` from 0.16.11 to 0.16.12
- Bump `org.apache.commons:commons-compress` from 1.22 to 1.23.0
- Bump `org.apache.commons:commons-configuration2` from 2.8.0 to 2.9.0
- Bump `com.netflix.nebula:nebula-publishing-plugin` from 19.2.0 to 20.3.0
- Bump `io.opencensus:opencensus-api` from 0.18.0 to 0.31.1 ([#7291](https://github.com/opensearch-project/OpenSearch/pull/7291))
- OpenJDK Update (April 2023 Patch releases) ([#7344](https://github.com/opensearch-project/OpenSearch/pull/7344)
- Bump `com.google.http-client:google-http-client:1.43.2` from 1.42.0 to 1.43.2 ([7928](https://github.com/opensearch-project/OpenSearch/pull/7928)))
- Add Opentelemetry dependencies ([#7543](https://github.com/opensearch-project/OpenSearch/issues/7543))
- Bump `org.bouncycastle:bcprov-jdk15on` to `org.bouncycastle:bcprov-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcmail-jdk15on` to `org.bouncycastle:bcmail-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump `org.bouncycastle:bcpkix-jdk15on` to `org.bouncycastle:bcpkix-jdk15to18` version 1.75 ([#8247](https://github.com/opensearch-project/OpenSearch/pull/8247))
- Bump JNA version from 5.5 to 5.13 ([#9963](https://github.com/opensearch-project/OpenSearch/pull/9963))

### Changed
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Don't over-allocate in HeapBufferedAsyncEntityConsumer in order to consume the response ([#9993](https://github.com/opensearch-project/OpenSearch/pull/9993))

### Security

## [Unreleased 2.x]
### Added
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))
- Add server version as REST response header [#6583](https://github.com/opensearch-project/OpenSearch/issues/6583)
- Start replication checkpointTimers on primary before segments upload to remote store. ([#8221](https://github.com/opensearch-project/OpenSearch/pull/8221))
- [distribution/archives] [Linux] [x64] Provide the variant of the distributions bundled with JRE ([#8195](https://github.com/opensearch-project/OpenSearch/pull/8195))
- Add configuration for file cache size to max remote data ratio to prevent oversubscription of file cache ([#8606](https://github.com/opensearch-project/OpenSearch/pull/8606))
- Disallow compression level to be set for default and best_compression index codecs ([#8737](https://github.com/opensearch-project/OpenSearch/pull/8737))
- Prioritize replica shard movement during shard relocation ([#8875](https://github.com/opensearch-project/OpenSearch/pull/8875))
- Introducing Default and Best Compression codecs as their algorithm name ([#9123](https://github.com/opensearch-project/OpenSearch/pull/9123))
- Make SearchTemplateRequest implement IndicesRequest.Replaceable ([#9122](https://github.com/opensearch-project/OpenSearch/pull/9122))
- [BWC and API enforcement] Define the initial set of annotations, their meaning and relations between them ([#9223](https://github.com/opensearch-project/OpenSearch/pull/9223))
- [Segment Replication] Support realtime reads for GET requests ([#9212](https://github.com/opensearch-project/OpenSearch/pull/9212))
- [Feature] Expose term frequency in Painless script score context ([#9081](https://github.com/opensearch-project/OpenSearch/pull/9081))
- Add support for reading partial files to HDFS repository ([#9513](https://github.com/opensearch-project/OpenSearch/issues/9513))
- Add support for extensions to search responses using SearchExtBuilder ([#9379](https://github.com/opensearch-project/OpenSearch/pull/9379))
- [Remote State] Create service to publish cluster state to remote store ([#9160](https://github.com/opensearch-project/OpenSearch/pull/9160))
- [BWC and API enforcement] Decorate the existing APIs with proper annotations (part 1) ([#9520](https://github.com/opensearch-project/OpenSearch/pull/9520))
- Add concurrent segment search related metrics to node and index stats ([#9622](https://github.com/opensearch-project/OpenSearch/issues/9622))
- Decouple replication lag from logic to fail stale replicas ([#9507](https://github.com/opensearch-project/OpenSearch/pull/9507))
- [Remote Store] Changes to introduce repository registration during bootstrap via node attributes. ([#9105](https://github.com/opensearch-project/OpenSearch/pull/9105))

### Dependencies
- Bump `peter-evans/create-or-update-comment` from 2 to 3 ([#9575](https://github.com/opensearch-project/OpenSearch/pull/9575))
- Bump `actions/checkout` from 2 to 4 ([#9968](https://github.com/opensearch-project/OpenSearch/pull/9968))
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973, #9972](https://github.com/opensearch-project/OpenSearch/pull/9973, https://github.com/opensearch-project/OpenSearch/pull/9972))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.1 to 2.23.0 ([#9971](https://github.com/opensearch-project/OpenSearch/pull/9971))

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))
- Mute the query profile IT with concurrent execution ([#9840](https://github.com/opensearch-project/OpenSearch/pull/9840))

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 3.0]: https://github.com/opensearch-project/OpenSearch/compare/2.x...HEAD
[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.11...2.x
