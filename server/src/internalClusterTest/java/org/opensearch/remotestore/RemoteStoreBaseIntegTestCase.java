/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.After;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class RemoteStoreBaseIntegTestCase extends OpenSearchIntegTestCase {
    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected static final String REPOSITORY_2_NAME = "test-remote-store-repo-2";
    protected static final int SHARD_COUNT = 1;
    protected static final int REPLICA_COUNT = 1;
    protected static final String TOTAL_OPERATIONS = "total-operations";
    protected static final String REFRESHED_OR_FLUSHED_OPERATIONS = "refreshed-or-flushed-operations";
    protected static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";
    protected static final String MAX_SEQ_NO_REFRESHED_OR_FLUSHED = "max-seq-no-refreshed-or-flushed";

    protected Path absolutePath;
    protected Path absolutePath2;
    private final List<String> documentKeys = List.of(
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5),
        randomAlphaOfLength(5)
    );

    protected Map<String, Long> indexData(int numberOfIterations, boolean invokeFlush, String index) {
        long totalOperations = 0;
        long refreshedOrFlushedOperations = 0;
        long maxSeqNo = -1;
        long maxSeqNoRefreshedOrFlushed = -1;
        int shardId = 0;
        Map<String, Long> indexingStats = new HashMap<>();
        for (int i = 0; i < numberOfIterations; i++) {
            if (invokeFlush) {
                flush(index);
            } else {
                refresh(index);
            }
            maxSeqNoRefreshedOrFlushed = maxSeqNo;
            indexingStats.put(MAX_SEQ_NO_REFRESHED_OR_FLUSHED + "-shard-" + shardId, maxSeqNoRefreshedOrFlushed);
            refreshedOrFlushedOperations = totalOperations;
            int numberOfOperations = randomIntBetween(20, 50);
            int numberOfBulk = randomIntBetween(1, 5);
            for (int j = 0; j < numberOfBulk; j++) {
                BulkResponse res = indexBulk(index, numberOfOperations);
                for (BulkItemResponse singleResp : res.getItems()) {
                    indexingStats.put(
                        MAX_SEQ_NO_TOTAL + "-shard-" + singleResp.getResponse().getShardId().id(),
                        singleResp.getResponse().getSeqNo()
                    );
                    maxSeqNo = singleResp.getResponse().getSeqNo();
                }
                totalOperations += numberOfOperations;
            }
        }

        indexingStats.put(TOTAL_OPERATIONS, totalOperations);
        indexingStats.put(REFRESHED_OR_FLUSHED_OPERATIONS, refreshedOrFlushedOperations);
        indexingStats.put(MAX_SEQ_NO_TOTAL, maxSeqNo);
        indexingStats.put(MAX_SEQ_NO_REFRESHED_OR_FLUSHED, maxSeqNoRefreshedOrFlushed);
        return indexingStats;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, REPOSITORY_2_NAME, true))
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder()
            .put(super.featureFlagSettings())
            .put(FeatureFlags.REMOTE_STORE, "true")
            .put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL, "true")
            .build();
    }

    public Settings indexSettings() {
        return defaultIndexSettings();
    }

    protected IndexResponse indexSingleDoc(String indexName) {
        return client().prepareIndex(indexName)
            .setId(UUIDs.randomBase64UUID())
            .setSource(documentKeys.get(randomIntBetween(0, documentKeys.size() - 1)), randomAlphaOfLength(5))
            .get();
    }

    protected BulkResponse indexBulk(String indexName, int numDocs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < numDocs; i++) {
            final IndexRequest request = client().prepareIndex(indexName)
                .setId(UUIDs.randomBase64UUID())
                .setSource(documentKeys.get(randomIntBetween(0, documentKeys.size() - 1)), randomAlphaOfLength(5))
                .request();
            bulkRequest.add(request);
        }
        return client().bulk(bulkRequest).actionGet();
    }

    public static Settings remoteStoreClusterSettings(String segmentRepoName) {
        return remoteStoreClusterSettings(segmentRepoName, segmentRepoName);
    }

    public static Settings remoteStoreClusterSettings(
        String segmentRepoName,
        String translogRepoName,
        boolean randomizeSameRepoForRSSAndRTS
    ) {
        return remoteStoreClusterSettings(
            segmentRepoName,
            randomizeSameRepoForRSSAndRTS ? (randomBoolean() ? translogRepoName : segmentRepoName) : translogRepoName
        );
    }

    public static Settings remoteStoreClusterSettings(String segmentRepoName, String translogRepoName) {
        return Settings.builder()
            .put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
            .put(CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(CLUSTER_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.getKey(), segmentRepoName)
            .put(CLUSTER_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey(), translogRepoName)
            .build();
    }

    private Settings defaultIndexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas, int numberOfShards) {
        return Settings.builder()
            .put(defaultIndexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas) {
        return remoteStoreIndexSettings(numberOfReplicas, 1);
    }

    protected Settings remoteStoreIndexSettings(int numberOfReplicas, long totalFieldLimit, int refresh) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(numberOfReplicas))
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldLimit)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), String.valueOf(refresh))
            .build();
    }

    protected void putRepository(Path path) {
        putRepository(path, REPOSITORY_NAME);
    }

    protected void putRepository(Path path, String repoName) {
        assertAcked(clusterAdmin().preparePutRepository(repoName).setType("fs").setSettings(Settings.builder().put("location", path)));
    }

    protected void setupRepo() {
        internalCluster().startClusterManagerOnlyNode();
        absolutePath = randomRepoPath().toAbsolutePath();
        putRepository(absolutePath);
        absolutePath2 = randomRepoPath().toAbsolutePath();
        putRepository(absolutePath2, REPOSITORY_2_NAME);
    }

    @After
    public void teardown() {
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_NAME));
        assertAcked(clusterAdmin().prepareDeleteRepository(REPOSITORY_2_NAME));
    }

    public static int getFileCount(Path path) throws Exception {
        final AtomicInteger filesExisting = new AtomicInteger(0);
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException impossible) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                filesExisting.incrementAndGet();
                return FileVisitResult.CONTINUE;
            }
        });

        return filesExisting.get();
    }

}
