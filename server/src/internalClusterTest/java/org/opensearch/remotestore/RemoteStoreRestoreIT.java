/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.compress.CompressorType;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.nio.file.Files.move;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreRestoreIT extends RemoteStoreBaseIntegTestCase {
    private static final String INDEX_NAME = "remote-store-test-idx-1";
    private static final String INDEX_NAMES = "test-remote-store-1,test-remote-store-2,remote-store-test-index-1,remote-store-test-index-2";
    private static final String INDEX_NAMES_WILDCARD = "test-remote-store-*,remote-store-test-index-*";
    private static final String TOTAL_OPERATIONS = "total-operations";
    private static final String REFRESHED_OR_FLUSHED_OPERATIONS = "refreshed-or-flushed-operations";
    private static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";
    private static final String MAX_SEQ_NO_REFRESHED_OR_FLUSHED = "max-seq-no-refreshed-or-flushed";

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    public Settings indexSettings(int shards, int replicas) {
        return remoteStoreIndexSettings(replicas, shards);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Before
    public void setup() {
        setupRepo();
    }

    private void restore(String... indices) {
        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(indices).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
    }

    private void verifyRestoredData(Map<String, Long> indexStats, boolean checkTotal, String indexName) {
        // This is required to get updated number from already active shards which were not restored
        refresh(indexName);
        String statsGranularity = checkTotal ? TOTAL_OPERATIONS : REFRESHED_OR_FLUSHED_OPERATIONS;
        String maxSeqNoGranularity = checkTotal ? MAX_SEQ_NO_TOTAL : MAX_SEQ_NO_REFRESHED_OR_FLUSHED;
        ensureYellowAndNoInitializingShards(indexName);
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), indexStats.get(statsGranularity));
        IndexResponse response = indexSingleDoc(indexName);
        assertEquals(indexStats.get(maxSeqNoGranularity + "-shard-" + response.getShardId().id()) + 1, response.getSeqNo());
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).setSize(0).get(), indexStats.get(statsGranularity) + 1);
    }

    private void prepareCluster(int numClusterManagerNodes, int numDataOnlyNodes, String indices, int replicaCount, int shardCount) {
        internalCluster().startClusterManagerOnlyNodes(numClusterManagerNodes);
        internalCluster().startDataOnlyNodes(numDataOnlyNodes);
        for (String index : indices.split(",")) {
            createIndex(index, remoteStoreIndexSettings(replicaCount, shardCount));
            ensureYellowAndNoInitializingShards(index);
            ensureGreen(index);
        }
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/6188")
    public void testRemoteTranslogRestoreWithNoDataPostCommit() throws IOException {
        testRestoreFlow(1, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithNoDataPostRefresh() throws IOException {
        testRestoreFlow(1, false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithRefreshedData() throws IOException {
        testRestoreFlow(randomIntBetween(2, 5), false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    public void testRemoteTranslogRestoreWithCommittedData() throws IOException {
        testRestoreFlow(randomIntBetween(2, 5), true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    // @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/6188")
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8479")
    public void testRTSRestoreWithNoDataPostCommitPrimaryReplicaDown() throws IOException {
        testRestoreFlowBothPrimaryReplicasDown(1, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates all data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8479")
    public void testRTSRestoreWithNoDataPostRefreshPrimaryReplicaDown() throws IOException {
        testRestoreFlowBothPrimaryReplicasDown(1, false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8479")
    public void testRTSRestoreWithRefreshedDataPrimaryReplicaDown() throws IOException {
        testRestoreFlowBothPrimaryReplicasDown(randomIntBetween(2, 5), false, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8479")
    public void testRTSRestoreWithCommittedDataPrimaryReplicaDown() throws IOException {
        testRestoreFlowBothPrimaryReplicasDown(randomIntBetween(2, 5), true, randomIntBetween(1, 5));
    }

    private void restoreAndVerify(int shardCount, int replicaCount, Map<String, Long> indexStats) {
        restore(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        // This is required to get updated number from already active shards which were not restored
        assertEquals(shardCount * (1 + replicaCount), getNumShards(INDEX_NAME).totalNumShards);
        assertEquals(replicaCount, getNumShards(INDEX_NAME).numReplicas);
        verifyRestoredData(indexStats, true, INDEX_NAME);
    }

    /**
     * Helper function to test restoring an index with no replication from remote store. Only primary node is dropped.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlow(int numberOfIterations, boolean invokeFlush, int shardCount) throws IOException {
        prepareCluster(0, 3, INDEX_NAME, 0, shardCount);
        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);

        restoreAndVerify(shardCount, 0, indexStats);
    }

    /**
     * Helper function to test restoring an index having replicas from remote store when all the nodes housing the primary/replica drop.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlowBothPrimaryReplicasDown(int numberOfIterations, boolean invokeFlush, int shardCount) throws IOException {
        prepareCluster(1, 2, INDEX_NAME, 1, shardCount);
        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(INDEX_NAME)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNodes(2);

        restoreAndVerify(shardCount, 1, indexStats);
    }

    /**
     * Helper function to test restoring multiple indices from remote store when all the nodes housing the primary/replica drop.
     * @param numberOfIterations Number of times a refresh/flush should be invoked, followed by indexing some data.
     * @param invokeFlush If true, a flush is invoked. Otherwise, a refresh is invoked.
     * @throws IOException IO Exception.
     */
    private void testRestoreFlowMultipleIndices(int numberOfIterations, boolean invokeFlush, int shardCount) throws IOException {
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            ClusterHealthStatus indexHealth = ensureRed(index);
            if (ClusterHealthStatus.RED.equals(indexHealth)) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(indexHealth)) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(INDEX_NAMES_WILDCARD.split(",")).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices);
        for (String index : indices) {
            assertEquals(shardCount, getNumShards(index).totalNumShards);
            verifyRestoredData(indicesStats.get(index), true, index);
        }
    }

    public void testRestoreFlowAllShardsNoRedIndex() throws InterruptedException {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(0, 3, INDEX_NAME, 0, shardCount);
        indexData(randomIntBetween(2, 5), true, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        PlainActionFuture<RestoreRemoteStoreResponse> future = PlainActionFuture.newFuture();
        client().admin().cluster().restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), future);
        try {
            future.get();
        } catch (ExecutionException e) {
            // If the request goes to co-ordinator, e.getCause() can be RemoteTransportException
            assertTrue(e.getCause() instanceof IllegalStateException || e.getCause().getCause() instanceof IllegalStateException);
        }
    }

    public void testRestoreFlowNoRedIndex() {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(0, 3, INDEX_NAME, 0, shardCount);
        Map<String, Long> indexStats = indexData(randomIntBetween(2, 5), true, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);

        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(false), PlainActionFuture.newFuture());

        ensureGreen(INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        verifyRestoredData(indexStats, true, INDEX_NAME);
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store
     * for multiple indices matching a wildcard name pattern.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataMultipleIndicesPatterns() throws IOException {
        testRestoreFlowMultipleIndices(2, true, randomIntBetween(1, 5));
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with all remote-enabled red indices considered for the restore by default.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataDefaultAllIndices() throws IOException {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(ensureRed(index))) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        restore(indices);
        ensureGreen(indices);

        for (String index : indices) {
            assertEquals(shardCount, getNumShards(index).totalNumShards);
            verifyRestoredData(indicesStats.get(index), true, index);
        }
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with only some of the remote-enabled red indices requested for the restore.
     * @throws IOException IO Exception.
     */
    public void testRTSRestoreWithCommittedDataNotAllRedRemoteIndices() throws IOException {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 0, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices[0], indices[1]));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(indices[0], indices[1]).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices[0], indices[1]);
        assertEquals(shardCount, getNumShards(indices[0]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[0]), true, indices[0]);
        assertEquals(shardCount, getNumShards(indices[1]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[1]), true, indices[1]);
        ensureRed(indices[2], indices[3]);
    }

    /**
     * Simulates refreshed data restored using Remote Segment Store
     * and unrefreshed data restored using Remote Translog Store,
     * with all remote-enabled red indices being considered for the restore
     * except those matching the specified exclusion pattern.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8480")
    public void testRTSRestoreWithCommittedDataExcludeIndicesPatterns() throws IOException {
        int shardCount = randomIntBetween(1, 5);
        prepareCluster(1, 3, INDEX_NAMES, 1, shardCount);
        String[] indices = INDEX_NAMES.split(",");
        Map<String, Map<String, Long>> indicesStats = new HashMap<>();
        for (String index : indices) {
            Map<String, Long> indexStats = indexData(2, true, index);
            indicesStats.put(index, indexStats);
            assertEquals(shardCount, getNumShards(index).totalNumShards);
        }

        for (String index : indices) {
            if (ClusterHealthStatus.RED.equals(ensureRed(index))) {
                continue;
            }

            if (ClusterHealthStatus.GREEN.equals(ensureRed(index))) {
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNodeName(index)));
            }

            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(index)));
        }

        ensureRed(indices);
        internalCluster().startDataOnlyNodes(3);

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(indices[0], indices[1]));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices("*", "-remote-store-test-index-*").restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(indices[0], indices[1]);
        assertEquals(shardCount, getNumShards(indices[0]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[0]), true, indices[0]);
        assertEquals(shardCount, getNumShards(indices[1]).totalNumShards);
        verifyRestoredData(indicesStats.get(indices[1]), true, indices[1]);
        ensureRed(indices[2], indices[3]);
    }

    /**
     * Simulates no-op restore from remote store,
     * when the index has no data.
     * @throws IOException IO Exception.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/6188")
    public void testRTSRestoreNoData() throws IOException {
        testRestoreFlow(0, true, randomIntBetween(1, 5));
    }

    // TODO: Restore flow - index aliases

    public void testRestoreFlowFullClusterRestart() {
        int shardCount = 1;
        // Step - 1 index some data to generate files in remote directory
        prepareCluster(0, 3, "my-index-01-orig", 0, shardCount);
        Map<String, Long> indexStats = indexData(1, false, "my-index-01-orig");
        assertEquals(shardCount, getNumShards("my-index-01-orig").totalNumShards);
        ensureGreen("my-index-01-orig");

        // Step - 2 copy the data generated in remote to a new indexuuid location. this indexuuid is the same uuid we get in remote
        // IndexMetadata
        try {
            move(
                absolutePath.resolve(clusterService().state().metadata().index("my-index-01-orig").getIndexUUID()),
                absolutePath.resolve("TLHafcwfTAazM5hFSFidyA")
            );
            move(
                absolutePath2.resolve(clusterService().state().metadata().index("my-index-01-orig").getIndexUUID()),
                absolutePath2.resolve("TLHafcwfTAazM5hFSFidyA")
            );
        } catch (NoSuchFileException e) {
            logger.info("using same repo");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Step - 3 Trigger full cluster restore
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().clusterUUID("sampleUUID"), PlainActionFuture.newFuture());

        // Step - 4 validation restore is successful.
        ensureGreen("my-index-01");
        assertEquals(getNumShards("my-index-01-orig").totalNumShards, getNumShards("my-index-01").totalNumShards);
        verifyRestoredData(indexStats, true, "my-index-01");
    }

    protected Settings.Builder getRepositorySettings(Path location, boolean shallowCopyEnabled) {
        Settings.Builder settingsBuilder = randomRepositorySettings();
        settingsBuilder.put("location", location);
        if (shallowCopyEnabled) {
            settingsBuilder.put(BlobStoreRepository.REMOTE_STORE_INDEX_SHALLOW_COPY.getKey(), true);
        }
        return settingsBuilder;
    }

    protected Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        final boolean compress = randomBoolean();
        settings.put("location", randomRepoPath()).put("compress", compress);
        if (compress) {
            settings.put("compression_type", randomFrom(CompressorType.values()));
        }
        if (rarely()) {
            settings.put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES);
        }
        return settings;
    }

    // public void testRestoreInSameRemoteStoreEnabledIndex() throws IOException {
    // String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
    // String primary = internalCluster().startDataOnlyNode();
    // String indexName1 = "testindex1";
    // // String indexName2 = "testindex2";
    // String snapshotRepoName = "test-restore-snapshot-repo";
    // String snapshotName1 = "test-restore-snapshot1";
    // // String snapshotName2 = "test-restore-snapshot2";
    // Path absolutePath1 = randomRepoPath().toAbsolutePath();
    // logger.info("Snapshot Path [{}]", absolutePath1);
    // // String restoredIndexName2 = indexName2 + "-restored";
    //
    // boolean enableShallowCopy = true;
    // assertAcked(
    // clusterAdmin().preparePutRepository(snapshotRepoName)
    // .setType("fs")
    // .setSettings(getRepositorySettings(absolutePath1, enableShallowCopy))
    // );
    // // createRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, enableShallowCopy));
    //
    // Client client = client();
    // Settings indexSettings = indexSettings(1, 0);
    // createIndex(indexName1, indexSettings);
    //
    // // Settings indexSettings2 = indexSettings(1, 0);
    // // createIndex(indexName2, indexSettings2);
    //
    // final int numDocsInIndex1 = 5;
    // // final int numDocsInIndex2 = 6;
    // indexData(numDocsInIndex1, true, indexName1);
    // // indexDocuments(client, indexName2, numDocsInIndex2);
    // ensureGreen(indexName1);
    //
    // internalCluster().startDataOnlyNode();
    // logger.info("--> snapshot");
    // CreateSnapshotResponse createSnapshotResponse = client.admin()
    // .cluster()
    // .prepareCreateSnapshot(snapshotRepoName, snapshotName1)
    // .setWaitForCompletion(true)
    // .setIndices(indexName1)
    // .get();
    // assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
    // assertThat(
    // createSnapshotResponse.getSnapshotInfo().successfulShards(),
    // equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
    // );
    // assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
    //
    // // updateRepository(snapshotRepoName, "fs", getRepositorySettings(absolutePath1, false));
    // // CreateSnapshotResponse createSnapshotResponse2 = client.admin()
    // // .cluster()
    // // .prepareCreateSnapshot(snapshotRepoName, snapshotName2)
    // // .setWaitForCompletion(true)
    // // .setIndices(indexName1, indexName2)
    // // .get();
    // // assertThat(createSnapshotResponse2.getSnapshotInfo().successfulShards(), greaterThan(0));
    // // assertThat(
    // // createSnapshotResponse2.getSnapshotInfo().successfulShards(),
    // // equalTo(createSnapshotResponse2.getSnapshotInfo().totalShards())
    // // );
    // // assertThat(createSnapshotResponse2.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
    //
    // // DeleteResponse deleteResponse = client().prepareDelete(indexName1, "0").execute().actionGet();
    // // assertEquals(deleteResponse.getResult(), DocWriteResponse.Result.DELETED);
    // // indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + randomIntBetween(2, 5));
    // // ensureGreen(indexName1);
    //
    // assertAcked(client().admin().indices().prepareDelete(indexName1));
    //
    // // RestoreSnapshotResponse restoreSnapshotResponse1 = client.admin()
    // // .cluster()
    // // .prepareRestoreSnapshot(snapshotRepoName, snapshotName1)
    // // .setWaitForCompletion(false)
    // // .setIndices(indexName1)
    // // .get();
    // // RestoreSnapshotResponse restoreSnapshotResponse2 = client.admin()
    // // .cluster()
    // // .prepareRestoreSnapshot(snapshotRepoName, snapshotName2)
    // // .setWaitForCompletion(false)
    // // .setIndices(indexName2)
    // // .setRenamePattern(indexName2)
    // // .setRenameReplacement(restoredIndexName2)
    // // .get();
    // // assertEquals(restoreSnapshotResponse1.status(), RestStatus.ACCEPTED);
    // // assertEquals(restoreSnapshotResponse2.status(), RestStatus.ACCEPTED);
    // // ensureGreen(indexName1, restoredIndexName2);
    // // assertDocsPresentInIndex(client, indexName1, numDocsInIndex1);
    // // assertDocsPresentInIndex(client, restoredIndexName2, numDocsInIndex2);
    //
    // // deleting data for restoredIndexName1 and restoring from remote store.
    // // internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
    // // ensureRed(indexName1);
    // // Re-initialize client to make sure we are not using client from stopped node.
    // client = client(clusterManagerNode);
    // // assertAcked(client.admin().indices().prepareClose(indexName1));
    // client.admin()
    // .cluster()
    // .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(indexName1).restoreAllShards(true), PlainActionFuture.newFuture());
    // // ensureYellowAndNoInitializingShards(indexName1);
    // ensureGreen("my-index-01");
    // // assertDocsPresentInIndex(client(), indexName1, numDocsInIndex1);
    // // indexing some new docs and validating
    // // indexDocuments(client, indexName1, numDocsInIndex1, numDocsInIndex1 + 2);
    // // ensureGreen(indexName1);
    // // assertDocsPresentInIndex(client, indexName1, numDocsInIndex1 + 2);
    // }
    //
}
