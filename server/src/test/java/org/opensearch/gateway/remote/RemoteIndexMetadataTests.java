/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata.COMPONENT_PREFIX;
import static org.opensearch.gateway.remote.RemoteIndexMetadata.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteIndexMetadata.INDEX_PATH_TOKEN;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

public class RemoteIndexMetadataTests extends OpenSearchTestCase {

    private static final String TEST_BLOB_NAME = "test-blob-name";
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
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForUpload.get(), is(indexMetadata));

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(TEST_BLOB_NAME, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForDownload.get(), nullValue());
    }

    public void testClusterUUID() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForUpload.clusterUUID(), is(clusterUUID));

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(TEST_BLOB_NAME, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForDownload.clusterUUID(), is(clusterUUID));
    }

    public void testFullBlobName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForUpload.getFullBlobName(), nullValue());
        // todo test after uploading as well

        RemoteIndexMetadata remoteObjectForDownload = new RemoteIndexMetadata(TEST_BLOB_NAME, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThat(remoteObjectForDownload.getFullBlobName(), is(TEST_BLOB_NAME));
    }

    public void testBlobPathParameters() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertThat(params.getPathTokens(), is(List.of(INDEX_PATH_TOKEN)));
        assertThat(params.getFilePrefix(),is("metadata"));
    }

    public void testGenerateBlobFileName() {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertThat(nameTokens[0] ,is("metadata"));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[1]) , is(VERSION));
        assertThat(RemoteStoreUtils.invertLong(nameTokens[2]) , lessThanOrEqualTo(System.currentTimeMillis()));
        assertThat(nameTokens[3] , is(String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION)));
    }

    public void testGetUploadedMetadata() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertThat(uploadedMetadata.getComponent() ,is(COMPONENT_PREFIX+"test-index"));
            assertThat(uploadedMetadata.getUploadedFilename() ,is(remoteObjectForUpload.getFullBlobName()));
        }
    }

    public void testSerDe() throws IOException {
        IndexMetadata indexMetadata = getIndexMetadata();
        RemoteIndexMetadata remoteObjectForUpload = new RemoteIndexMetadata(indexMetadata, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            assertThat(inputStream.available(), greaterThan(0));
            IndexMetadata readIndexMetadata = remoteObjectForUpload.deserialize(inputStream);
            assertThat(readIndexMetadata, is(indexMetadata));
        }
    }

    private IndexMetadata getIndexMetadata() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        return new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .version(VERSION)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
