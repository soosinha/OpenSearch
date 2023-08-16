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
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STATE_REPOSITORY_SETTING;
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
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME))
            .put(CLUSTER_REMOTE_STATE_REPOSITORY_SETTING.getKey(), REPOSITORY_NAME)
            .build();
    }

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

    public void testRestoreFlowFullClusterRestartZeroReplica() {
        int shardCount = 1;
        // Step - 1 index some data to generate files in remote directory
        prepareCluster(0, 1, INDEX_NAME, 0, shardCount);
        Map<String, Long> indexStats = indexData(1, false, INDEX_NAME);
        assertEquals(shardCount, getNumShards(INDEX_NAME).totalNumShards);
        ensureGreen(INDEX_NAME);
        String prevClusterUUID = clusterService().state().metadata().clusterUUID();

        // Step - 2 Perform full cluster restart. This ensures new cluster state doesnt have previous index metadata
        try {
            internalCluster().stopRandomDataNode();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            internalCluster().stopCurrentClusterManagerNode();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        internalCluster().startNode();
        putRepository(absolutePath);
        putRepository(absolutePath2, REPOSITORY_2_NAME);

        String newClusterUUID = clusterService().state().metadata().clusterUUID();
        assert !Objects.equals(newClusterUUID, prevClusterUUID) : "cluster restart not successful. cluster uuid is same";

        // Step - 3 Trigger full cluster restore
        client().admin()
            .cluster()
            // Any sampleUUID would work as we are not integrated with remote cluster state repo in this test.
            // We are mocking that interaction and supplying dummy index metadata
            .restoreRemoteStore(new RestoreRemoteStoreRequest().clusterUUID(prevClusterUUID), PlainActionFuture.newFuture());

        // Step - 4 validation restore is successful.
        ensureGreen(INDEX_NAME);
        assertEquals(getNumShards(INDEX_NAME).totalNumShards, getNumShards(INDEX_NAME).totalNumShards);
        verifyRestoredData(indexStats, true, INDEX_NAME);
    }
}
