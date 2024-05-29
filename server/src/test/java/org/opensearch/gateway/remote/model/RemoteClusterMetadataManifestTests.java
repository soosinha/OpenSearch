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
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST_FILE_PREFIX;
import static org.opensearch.gateway.remote.model.RemoteClusterMetadataManifest.MANIFEST_PATH_TOKEN;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.ClusterStateDiffManifest;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RemoteClusterMetadataManifestTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";

    private static final long TERM = 2L;
    private static final long VERSION = 5L;

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
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.get(), is(manifest));

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.get(), nullValue());
    }

    public void testClusterUUID() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForUpload.getBlobFileName(), nullValue());

        RemoteClusterMetadataManifest remoteObjectForDownload = new RemoteClusterMetadataManifest(TEST_BLOB_NAME, clusterUUID, blobStoreRepository);
        assertThat(remoteObjectForDownload.getBlobFileName(), is(TEST_BLOB_FILE_NAME));
    }

    public void testBlobPathParameters() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(MANIFEST_PATH_TOKEN)));
        assertThat(params.getFilePrefix(), is(MANIFEST_FILE_PREFIX));
    }

    public void testGenerateBlobFileName() {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0], is(MANIFEST_FILE_PREFIX));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]), is(TERM));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]), is(VERSION));
        assertThat(nameTokens[3], is("C"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[4]), lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[5], is(String.valueOf(MANIFEST_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent(), is(MANIFEST_PATH_TOKEN));
            assertThat(uploadedMetadata.getUploadedFilename(), is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        ClusterMetadataManifest manifest = getClusterMetadataManifest();
        RemoteClusterMetadataManifest remoteObjectForUpload = new RemoteClusterMetadataManifest(manifest, clusterUUID, blobStoreRepository);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertThat(inputStream.available(), greaterThan(0));
            ClusterMetadataManifest readManifest = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readManifest, is(manifest));
        }
    }

    private ClusterMetadataManifest getClusterMetadataManifest() {
        return ClusterMetadataManifest.builder().opensearchVersion(Version.CURRENT).codecVersion(MANIFEST_CURRENT_CODEC_VERSION).nodeId("test-node")
            .clusterUUID("test-uuid").previousClusterUUID("_NA_").stateUUID("state-uuid").clusterTerm(TERM).stateVersion(VERSION).committed(true)
            .coordinationMetadata(new UploadedMetadataAttribute("test-attr", "uploaded-file")).diffManifest(
                ClusterStateDiffManifest.builder().fromStateUUID("from-uuid").toStateUUID("to-uuid").indicesUpdated(Collections.emptyList())
                    .indicesDeleted(Collections.emptyList())
                    .customMetadataUpdated(Collections.emptyList()).customMetadataDeleted(Collections.emptyList())
                    .indicesRoutingUpdated(Collections.emptyList()).indicesRoutingDeleted(Collections.emptyList()).build()).build();
    }
}
