/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 */

package org.opensearch.cluster.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.store.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService {

    public static final String METADATA_NAME_FORMAT = "%s.dat";

    public static final String METADATA_MARKER_NAME_FORMAT = "%s";

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<ClusterMetadataMarker> CLUSTER_METADATA_MARKER_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-marker",
        METADATA_MARKER_NAME_FORMAT,
        ClusterMetadataMarker::fromXContent
    );
    /**
     * Used to specify if all indexes are to create with remote store enabled by default
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );
    /**
     * Used to specify default repo to use for translog upload for remote store backed indices
     */
    public static final Setting<String> REMOTE_CLUSTER_STATE_REPOSITORY_SETTING = Setting.simpleString(
        "cluster.remote_store.state.repository",
        "",
        Property.NodeScope,
        Property.Final
    );
    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    private static final String DELIMITER = "__";

    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private BlobStoreRepository blobStoreRepository;

    public RemoteClusterStateService(Supplier<RepositoriesService> repositoriesService, Settings settings) {
        this.repositoriesService = repositoriesService;
        this.settings = settings;
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store.
     * For now only index metadata upload is supported.
     * This method should be invoked by the elected cluster manager when the remote cluster
     * state is enabled.
     * @param clusterState
     * @return A metadata/marker object which contains the details of uploaded entity metadata.
     * @throws IOException
     */
    @Nullable
    public ClusterMetadataMarker writeFullMetadata(ClusterState clusterState) throws IOException {
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";
        initializeRepository();

        final Map<String, ClusterMetadataMarker.UploadedIndexMetadata> allUploadedIndexMetadata = new HashMap<>();
        // todo parallel upload
        // any validations before/after upload ?
        for (IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX/metadata_4_1690947200
            final String indexMetadataKey = writeIndexMetadata(
                clusterState.getClusterName().value(),
                clusterState.getMetadata().clusterUUID(),
                indexMetadata,
                indexMetadataFileName(indexMetadata)
            );
            final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(
                indexMetadata.getIndex().getName(),
                indexMetadata.getIndexUUID(),
                indexMetadataKey
            );
            allUploadedIndexMetadata.put(indexMetadata.getIndex().getName(), uploadedIndexMetadata);
        }
        return uploadMarker(clusterState, allUploadedIndexMetadata, false);
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state.
     * The previous marker file is needed to create the new marker. The new marker file is created by using
     * the unchanged metadata from the previous marker and the new metadata changes from the current cluster state.
     * @param previousClusterState
     * @param clusterState
     * @param previousMarker
     * @return The uploaded ClusterMetadataMarker file
     * @throws IOException
     */
    @Nullable
    public ClusterMetadataMarker writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataMarker previousMarker
    ) throws IOException {
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";
        final Map<String, Long> previousStateIndexMetadataVersionByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : previousClusterState.metadata().indices().values()) {
            previousStateIndexMetadataVersionByName.put(indexMetadata.getIndex().getName(), indexMetadata.getVersion());
        }

        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataMarker.UploadedIndexMetadata> allUploadedIndexMetadata = new HashMap<>(
            previousMarker.getIndices()
        );
        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            final Long previousVersion = previousStateIndexMetadataVersionByName.get(indexMetadata.getIndex().getName());
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.trace(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                final String indexMetadataKey = writeIndexMetadata(
                    clusterState.getClusterName().value(),
                    clusterState.getMetadata().clusterUUID(),
                    indexMetadata,
                    indexMetadataFileName(indexMetadata)
                );
                final UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataKey
                );
                allUploadedIndexMetadata.put(indexMetadata.getIndex().getName(), uploadedIndexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            previousStateIndexMetadataVersionByName.remove(indexMetadata.getIndex().getName());
        }

        for (String removedIndexName : previousStateIndexMetadataVersionByName.keySet()) {
            allUploadedIndexMetadata.remove(removedIndexName);
        }
        return uploadMarker(clusterState, allUploadedIndexMetadata, false);
    }

    public ClusterMetadataMarker markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataMarker previousMarker)
        throws IOException {
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert clusterState != null : "Last accepted cluster state is not set";
        assert previousMarker != null : "Last cluster metadata marker is not set";
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";
        return uploadMarker(clusterState, previousMarker.getIndices(), true);
    }

    public ClusterState getLatestClusterState(String clusterUUID) {
        // todo
        return null;
    }

    // Visible for testing
    void initializeRepository() {
        if (blobStoreRepository != null) {
            return;
        }
        final String remoteStoreRepo = REMOTE_CLUSTER_STATE_REPOSITORY_SETTING.get(settings);
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    private ClusterMetadataMarker uploadMarker(
        ClusterState clusterState,
        Map<String, ClusterMetadataMarker.UploadedIndexMetadata> uploadedIndexMetadata,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String markerFileName = getMarkerFileName(clusterState.term(), clusterState.version());
            final ClusterMetadataMarker marker = new ClusterMetadataMarker(
                uploadedIndexMetadata,
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                committed
            );
            writeMetadataMarker(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), marker, markerFileName);
            return marker;
        }
    }

    private String writeIndexMetadata(String clusterName, String clusterUUID, IndexMetadata uploadIndexMetadata, String fileName)
        throws IOException {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(clusterName, clusterUUID, uploadIndexMetadata.getIndexUUID());
        INDEX_METADATA_FORMAT.write(uploadIndexMetadata, indexMetadataContainer, fileName, blobStoreRepository.getCompressor());
        // returning full path
        return indexMetadataContainer.path().buildAsString() + fileName;
    }

    private void writeMetadataMarker(String clusterName, String clusterUUID, ClusterMetadataMarker uploadMarker, String fileName)
        throws IOException {
        final BlobContainer metadataMarkerContainer = markerContainer(clusterName, clusterUUID);
        CLUSTER_METADATA_MARKER_FORMAT.write(uploadMarker, metadataMarkerContainer, fileName, blobStoreRepository.getCompressor());
    }

    private BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add("cluster-state")
                    .add(clusterUUID)
                    .add("index")
                    .add(indexUUID)
            );
    }

    private BlobContainer markerContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/marker
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add("cluster-state")
                    .add(clusterUUID)
                    .add("marker")
            );
    }

    private static String getMarkerFileName(long term, long version) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/marker/2147483642_2147483637_456536447_marker
        return String.join(
            DELIMITER,
            "marker",
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            RemoteStoreUtils.invertLong(System.currentTimeMillis())
        );
    }

    private static String indexMetadataFileName(IndexMetadata indexMetadata) {
        return String.join(DELIMITER, "metadata", String.valueOf(indexMetadata.getVersion()), String.valueOf(System.currentTimeMillis()));
    }

    public ClusterMetadataMarker getLatestClusterMetadataMarker(String clusterUUID, String clusterState) {
        String latestMarkerFileName = getLatestMarkerFileName(clusterUUID, clusterState);
        return fetchRemoteClusterMetadataMarker(latestMarkerFileName, clusterUUID, clusterState);
    }

    public ClusterMetadataMarker fetchRemoteClusterMetadataMarker(String filename, String clusterUUID, String clusterState)
        throws IllegalStateException {
        try {
            return RemoteClusterStateService.CLUSTER_METADATA_MARKER_FORMAT.read(
                markerContainer(clusterUUID, clusterState),
                filename,
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            String errorMsg = String.format(Locale.ROOT, "Error while downloading ClusterMetadataMarker - %s", filename);
            logger.error(errorMsg, e);
            throw new IllegalStateException(errorMsg);
        }
    }

    public String getLatestMarkerFileName(String clusterUUID, String clusterState) throws IllegalStateException {
        try {
            List<BlobMetadata> markerFilesMetadata = markerContainer(clusterUUID, clusterState).listBlobsByPrefixInSortedOrder(
                "marker",
                1,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
            if (markerFilesMetadata != null && !markerFilesMetadata.isEmpty()) {
                return markerFilesMetadata.get(0).name();
            }
        } catch (IOException e) {
            String errorMsg = "Error while fetching latest marker file for remote cluster state";
            logger.error(errorMsg, e);
            throw new IllegalStateException(errorMsg);
        }

        throw new IllegalStateException("Remote Cluster State not found");
    }

    /**
     * Fetch latest index metadata which is part of remote cluster state
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest IndexMetadata map
     */
    public Map<String, IndexMetadata> getLatestIndexMetadata(String clusterUUID, String clusterName) throws IOException {
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        ClusterMetadataMarker clusterMetadataMarker = getLatestClusterMetadataMarker(clusterUUID, clusterName);
        for (Map.Entry<String, UploadedIndexMetadata> entry : clusterMetadataMarker.getIndices().entrySet()) {
            IndexMetadata indexMetadata = INDEX_METADATA_FORMAT.read(
                indexMetadataContainer(clusterName, clusterUUID, entry.getValue().getIndexUUID()),
                entry.getValue().getUploadedFilename(),
                blobStoreRepository.getNamedXContentRegistry()
            );
            remoteIndexMetadata.put(entry.getKey(), indexMetadata);
        }
        return remoteIndexMetadata;
    }
}
