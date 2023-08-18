/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.cluster.store;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.store.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.FilterRepository;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import org.mockito.ArgumentMatchers;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterStateServiceTests extends OpenSearchTestCase {

    private RemoteClusterStateService remoteClusterStateService;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository blobStoreRepository;

    @Before
    public void setup() {
        repositoriesServiceSupplier = mock(Supplier.class);
        repositoriesService = mock(RepositoriesService.class);
        when(repositoriesServiceSupplier.get()).thenReturn(repositoriesService);
        final Settings settings = Settings.builder()
            .put(IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
            .put(IndicesService.CLUSTER_REMOTE_STATE_REPOSITORY_SETTING.getKey(), "remote_store_repository")
            .build();
        blobStoreRepository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(blobStoreRepository);
        remoteClusterStateService = new RemoteClusterStateService(repositoriesServiceSupplier, settings);
    }

    public void testFailWriteFullMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterState();
        final ClusterMetadataMarker marker = remoteClusterStateService.writeFullMetadata(clusterState);
        Assert.assertThat(marker, nullValue());
    }

    public void testFailWriteFullMetadataWhenRemoteStateDisabled() throws IOException {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final Settings settings = Settings.builder().build();
        remoteClusterStateService = spy(new RemoteClusterStateService(repositoriesServiceSupplier, settings));
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        assertThrows(AssertionError.class, () -> remoteClusterStateService.writeFullMetadata(clusterState));
    }

    public void testFailWriteFullMetadataWhenRepositoryNotSet() {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        doThrow(new RepositoryMissingException("repository missing")).when(repositoriesService).repository("remote_store_repository");
        assertThrows(RepositoryMissingException.class, () -> remoteClusterStateService.writeFullMetadata(clusterState));
    }

    public void testFailWriteFullMetadataWhenNotBlobRepository() {
        final FilterRepository filterRepository = mock(FilterRepository.class);
        when(repositoriesService.repository("remote_store_repository")).thenReturn(filterRepository);
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        assertThrows(AssertionError.class, () -> remoteClusterStateService.writeFullMetadata(clusterState));
    }

    public void testWriteFullMetadataSuccess() throws IOException {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        final BlobStore blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(ArgumentMatchers.any())).thenReturn(blobContainer);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());

        final ClusterMetadataMarker marker = remoteClusterStateService.writeFullMetadata(clusterState);
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        Map<String, UploadedIndexMetadata> indices = Map.of("test-index", uploadedIndexMetadata);

        final ClusterMetadataMarker expectedMarker = ClusterMetadataMarker.builder()
            .indices(indices)
            .term(1L)
            .version(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .build();

        assertThat(marker.getIndices().size(), is(1));
        assertThat(marker.getIndices().get("test-index").getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(marker.getIndices().get("test-index").getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(marker.getIndices().get("test-index").getUploadedFilename(), notNullValue());
        assertThat(marker.getTerm(), is(expectedMarker.getTerm()));
        assertThat(marker.getVersion(), is(expectedMarker.getVersion()));
        assertThat(marker.getClusterUUID(), is(expectedMarker.getClusterUUID()));
        assertThat(marker.getStateUUID(), is(expectedMarker.getStateUUID()));
    }

    public void testFailWriteIncrementalMetadataNonClusterManagerNode() throws IOException {
        final ClusterState clusterState = generateClusterState();
        final ClusterMetadataMarker marker = remoteClusterStateService.writeIncrementalMetadata(clusterState, clusterState, null);
        Assert.assertThat(marker, nullValue());
    }

    public void testFailWriteIncrementalMetadataWhenTermChanged() {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(2L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();
        assertThrows(
            AssertionError.class,
            () -> remoteClusterStateService.writeIncrementalMetadata(previousClusterState, clusterState, null)
        );
    }

    public void testWriteIncrementalMetadataSuccess() throws IOException {
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .clusterManagerNodeId("cluster-manager-id")
            .localNodeId("cluster-manager-id")
            .build();
        final ClusterState clusterState = ClusterState.builder(generateClusterState()).nodes(nodes).build();
        final BlobStore blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(ArgumentMatchers.any())).thenReturn(blobContainer);
        when(blobStoreRepository.getCompressor()).thenReturn(new DeflateCompressor());

        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final ClusterState previousClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().coordinationMetadata(coordinationMetadata))
            .build();

        final ClusterMetadataMarker previousMarker = ClusterMetadataMarker.builder().indices(Collections.emptyMap()).build();

        remoteClusterStateService.initializeRepository();
        final ClusterMetadataMarker marker = remoteClusterStateService.writeIncrementalMetadata(
            previousClusterState,
            clusterState,
            previousMarker
        );
        final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata("test-index", "index-uuid", "metadata-filename");
        Map<String, UploadedIndexMetadata> indices = Map.of("test-index", uploadedIndexMetadata);

        final ClusterMetadataMarker expectedMarker = ClusterMetadataMarker.builder()
            .indices(indices)
            .term(1L)
            .version(1L)
            .stateUUID("state-uuid")
            .clusterUUID("cluster-uuid")
            .build();

        assertThat(marker.getIndices().size(), is(1));
        assertThat(marker.getIndices().get("test-index").getIndexName(), is(uploadedIndexMetadata.getIndexName()));
        assertThat(marker.getIndices().get("test-index").getIndexUUID(), is(uploadedIndexMetadata.getIndexUUID()));
        assertThat(marker.getIndices().get("test-index").getUploadedFilename(), notNullValue());
        assertThat(marker.getTerm(), is(expectedMarker.getTerm()));
        assertThat(marker.getVersion(), is(expectedMarker.getVersion()));
        assertThat(marker.getClusterUUID(), is(expectedMarker.getClusterUUID()));
        assertThat(marker.getStateUUID(), is(expectedMarker.getStateUUID()));
    }

    private static ClusterState generateClusterState() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder().put(indexMetadata, true).clusterUUID("cluster-uuid").coordinationMetadata(coordinationMetadata).build()
            )
            .build();
    }

}
