package org.opensearch.cluster.store;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_STATE_REPOSITORY_SETTING;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.store.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService {

    public static final String METADATA_NAME_FORMAT = "meta-%s.dat";

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
    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    private static final String DELIMITER = "__";

    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private BlobStoreRepository blobStoreRepository;

    public RemoteClusterStateService(Supplier<RepositoriesService> repositoriesService, Settings settings) {
        this.repositoriesService = repositoriesService;
        this.settings = settings;
    }

    public void writeFullMetadata(long currentTerm, ClusterState clusterState) throws IOException {
        if (!clusterState.nodes().isLocalNodeElectedClusterManager()) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return;
        }
        setRepository();
        if (blobStoreRepository == null) {
            logger.error("Unable to set repository");
            return;
        }

        final Map<String, ClusterMetadataMarker.UploadedIndexMetadata> allUploadedIndexMetadata = new HashMap<>();
        //todo parallel upload
        // any validations before/after upload ?
        for (IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX/metadata_4_1690947200
            String indexMetadataKey = writeIndexMetadata(clusterState.getClusterName().value(), clusterState.getMetadata().clusterUUID(),
                indexMetadata, indexMetadataFileName(indexMetadata));
            UploadedIndexMetadata uploadedIndexMetadata = new UploadedIndexMetadata(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID(),
                indexMetadataKey);
            allUploadedIndexMetadata.put(indexMetadata.getIndex().getName(), uploadedIndexMetadata);
        }
        uploadMarker(clusterState, allUploadedIndexMetadata);
    }

    private void setRepository() {
        try {
            if (blobStoreRepository != null) {
                return;
            }
            if (IndicesService.CLUSTER_REMOTE_STORE_ENABLED_SETTING.get(settings)) {
                String remoteStoreRepo = CLUSTER_REMOTE_STATE_REPOSITORY_SETTING.get(settings);
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

    public void writeIncrementalMetadata(long currentTerm, ClusterState previousClusterState, ClusterState clusterState) {
        //todo
    }

    public ClusterState getLatestClusterState(String clusterUUID) {
        //todo
        return null;
    }

    //todo exception handling
    public void uploadMarker(ClusterState clusterState, Map<String, ClusterMetadataMarker.UploadedIndexMetadata> uploadedIndexMetadata) throws IOException {
        synchronized (this) {
            String markerFileName = getMarkerFileName(clusterState.term(), clusterState.version());
            ClusterMetadataMarker marker = new ClusterMetadataMarker(uploadedIndexMetadata, clusterState.term(), clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID());
            writeMetadataMarker(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), marker, markerFileName);
        }
    }

    public String writeIndexMetadata(String clusterName, String clusterUUID, IndexMetadata indexMetadata, String fileName) throws IOException {
        BlobContainer indexMetadataContainer = indexMetadataContainer(clusterName, clusterUUID, indexMetadata.getIndexUUID());
        INDEX_METADATA_FORMAT.write(indexMetadata, indexMetadataContainer, fileName, blobStoreRepository.getCompressor());
        // returning full path
        return indexMetadataContainer.path().buildAsString() + fileName;
    }

    public void writeMetadataMarker(String clusterName, String clusterUUID, ClusterMetadataMarker marker, String fileName) throws IOException {
        BlobContainer metadataMarkerContainer = markerContainer(clusterName, clusterUUID);
        RemoteClusterStateService.CLUSTER_METADATA_MARKER_FORMAT.write(marker, metadataMarkerContainer, fileName, blobStoreRepository.getCompressor());
    }

    private static String getMarkerFileName(long term, long version) {
        //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/marker/2147483642_2147483637_456536447_marker
        return String.join(DELIMITER, String.valueOf(Long.MAX_VALUE - term), String.valueOf(Long.MAX_VALUE - version),
            String.valueOf(Long.MAX_VALUE - System.currentTimeMillis()), "marker");
    }


    private static String indexMetadataFileName(IndexMetadata indexMetadata) {
        return String.join(DELIMITER, "metadata", String.valueOf(indexMetadata.getVersion()), String.valueOf(System.currentTimeMillis()));
    }

    public BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(blobStoreRepository.basePath().add(clusterName).add("cluster-state").add(clusterUUID).add("index").add(indexUUID));
    }

    public BlobContainer markerContainer(String clusterName, String clusterUUID) {
        //123456789012_test-cluster/cluster-state/dsgYj10Nkso7/marker
        return blobStoreRepository.blobStore()
            .blobContainer(blobStoreRepository.basePath().add(clusterName).add("cluster-state").add(clusterUUID).add("marker"));
    }


}
