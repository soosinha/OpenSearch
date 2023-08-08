package org.opensearch.cluster.store;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STORE_REPOSITORY_SETTING;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

public class RemoteClusterStateService {

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    private static final String DELIMITER = "__";

    private final Supplier<RepositoriesService> repositoriesService;
    private final ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;

    public RemoteClusterStateService(Supplier<RepositoriesService> repositoriesService, ClusterSettings clusterSettings) {
        this.repositoriesService = repositoriesService;
        this.clusterSettings = clusterSettings;
    }

    public void writeFullStateAndCommit(long currentTerm, ClusterState clusterState) throws IOException {
        if (!clusterState.nodes().isLocalNodeElectedClusterManager()) {
            logger.error("local node is not electer cluster manager. Exiting");
            return;
        }
        setRepository();
        if (blobStoreRepository == null) {
            logger.error("Unable to set repository");
            return;
        }

        Map<String, String> indexMetadataKeys = new HashMap<>();
        for (IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX/metadata_4_1690947200
            String indexMetadataKey = blobStoreRepository.writeIndexMetadata(clusterState.getClusterName().value(), clusterState.getMetadata().clusterUUID(),
                indexMetadata, indexMetadataFileName(indexMetadata));
            indexMetadataKeys.put(indexMetadata.getIndex().getName(), indexMetadataKey);
        }
        uploadMarker(clusterState, indexMetadataKeys);
    }

    private void setRepository() {
        try {
            if (blobStoreRepository != null) {
                return;
            }
            if (clusterSettings.get(IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING)) {
                String remoteStoreRepo = clusterSettings.get(CLUSTER_REMOTE_STORE_REPOSITORY_SETTING);
                Repository repository = repositoriesService.get().repository(remoteStoreRepo);
                assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
                blobStoreRepository = (BlobStoreRepository) repository;
            } else {
                logger.info("remote store is not enabled");
            }
        } catch (Exception e) {
            logger.error("set repo exception", e);
        }
    }

    public void writeIncrementalStateAndCommit(long currentTerm, ClusterState previousClusterState, ClusterState clusterState) {
        //todo
    }

    // why do we need this ?
    public void writeIncrementalTermUpdateAndCommit(long currentTerm, long lastAcceptedVersion) {
        //todo
    }

    public ClusterState getLatestClusterState(String clusterUUID) {
        //todo
        return null;
    }

    //todo exception handling
    public void uploadMarker(ClusterState clusterState, Map<String, String> indexMetadataKeys) throws IOException {
        synchronized (this) {
            String markerFileName = getMarkerFileName(clusterState.term(), clusterState.version());
            Map<String, ClusterMetadataMarker.UploadedIndexMetadata> uploadedIndices = indexMetadataKeys.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ClusterMetadataMarker.UploadedIndexMetadata(e.getValue())));
            ClusterMetadataMarker marker = new ClusterMetadataMarker(uploadedIndices, clusterState.term(), clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID());
            blobStoreRepository.writeMetadataMarker(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), marker, markerFileName);
        }
    }

    private static String getMarkerFileName(long term, long version) {
        //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/marker/2147483642_2147483637_456536447_marker
        return String.join(DELIMITER, String.valueOf(Long.MAX_VALUE - term), String.valueOf(Long.MAX_VALUE - version),
            String.valueOf(Long.MAX_VALUE - System.currentTimeMillis()), "marker");
    }


    private static String indexMetadataFileName(IndexMetadata indexMetadata) {
        return String.join(DELIMITER, "metadata", String.valueOf(indexMetadata.getVersion()), String.valueOf(System.currentTimeMillis()));
    }


}
