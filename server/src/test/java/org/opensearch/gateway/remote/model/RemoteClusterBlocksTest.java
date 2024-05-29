/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS;

import java.io.IOException;
import java.io.InputStream;
import java.util.EnumSet;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RemoteClusterBlocksTest extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long VERSION = 5L;
    private static final String PATH_PARAM_TRANSIENT = "transient";

    private String clusterUUID;
    private BlobStoreTransferService blobStoreTransferService;
    private BlobStoreRepository blobStoreRepository;
    private String clusterName;
    private ClusterSettings clusterSettings;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        this.clusterUUID = "test-cluster-uuid";
        this.blobStoreTransferService = mock(BlobStoreTransferService.class);
        this.blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("/path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());
        this.clusterName = "test-cluster-name";
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGet() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.get(), is(clusterBlocks));

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.get(), nullValue());
    }

    public void testClusterUUID() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(PATH_PARAM_TRANSIENT)));
        assertThat(params.getFilePrefix(), is(CLUSTER_BLOCKS));
    }

    public void testGenerateBlobFileName() {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(CLUSTER_BLOCKS));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3], is(String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(CLUSTER_BLOCKS));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        ClusterBlocks clusterBlocks = getClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, VERSION, clusterUUID, blobStoreRepository);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            assertThat(inputStream.available(), greaterThan(0));
            ClusterBlocks readClusterBlocks = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readClusterBlocks.global().size(), is(1));
            assertThat(readClusterBlocks.global().iterator().next(), is(clusterBlocks.global().iterator().next()));
        }
    }

    private ClusterBlocks getClusterBlocks() {
        return new ClusterBlocks.Builder().addGlobalBlock(new ClusterBlock(12345, "test block", true, false, true, RestStatus.CREATED, EnumSet.of(
            ClusterBlockLevel.WRITE))).build();
    }
}
