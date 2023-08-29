/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataMarker.UploadedIndexMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {

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
     * Used to specify if cluster state metadata should be publish to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );
    /**
     * Used to specify default repo to use for cluster state metadata upload
     */
    public static final Setting<String> REMOTE_CLUSTER_STATE_REPOSITORY_SETTING = Setting.simpleString(
        "cluster.remote_store.state.repository",
        "",
        Property.NodeScope,
        Property.Final
    );
    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    private static final String DELIMITER = "__";

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeMillisSupplier;
    private BlobStoreRepository blobStoreRepository;
    private volatile TimeValue slowWriteLoggingThreshold;

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        LongSupplier relativeTimeMillisSupplier
    ) {
        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeMillisSupplier = relativeTimeMillisSupplier;
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A metadata/marker object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataMarker writeFullMetadata(ClusterState clusterState) throws IOException {
        final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings) == true : "Remote cluster state is not enabled";

        // any validations before/after upload ?
        final List<UploadedIndexMetadata> allUploadedIndexMetadata;
        try {
            allUploadedIndexMetadata = writeIndexMetadata(clusterState, new ArrayList<>(clusterState.metadata().indices().values()));
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        final ClusterMetadataMarker marker = uploadMarker(clusterState, allUploadedIndexMetadata, false);
        final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                allUploadedIndexMetadata.size()
            );
        } else {
            // todo change to debug
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices",
                durationMillis,
                allUploadedIndexMetadata.size()
            );
        }
        return marker;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous marker file is needed to create the new
     * marker. The new marker file is created by using the unchanged metadata from the previous marker and the new metadata changes from the current cluster
     * state.
     *
     * @return The uploaded ClusterMetadataMarker file
     */
    @Nullable
    public ClusterMetadataMarker writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataMarker previousMarker
    ) throws IOException {
        final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
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
        final Map<String, ClusterMetadataMarker.UploadedIndexMetadata> allUploadedIndexMetadata = previousMarker.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();

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
                toUpload.add(indexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            previousStateIndexMetadataVersionByName.remove(indexMetadata.getIndex().getName());
        }

        try {
            writeIndexMetadata(clusterState, toUpload);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        for (String removedIndexName : previousStateIndexMetadataVersionByName.keySet()) {
            allUploadedIndexMetadata.remove(removedIndexName);
        }
        final ClusterMetadataMarker marker = uploadMarker(clusterState, new ArrayList<>(allUploadedIndexMetadata.values()), false);
        final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        } else {
            // todo change to debug
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote and metadata for [{}] indices and skipped [{}] unchanged indices",
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged
            );
        }
        return marker;
    }

    private List<UploadedIndexMetadata> writeIndexMetadata(ClusterState clusterState, List<IndexMetadata> toUpload) throws IOException,
        InterruptedException, TimeoutException {
        List<Exception> exceptionList = new ArrayList<>(toUpload.size());
        final CountDownLatch latch = new CountDownLatch(toUpload.size());
        List<UploadedIndexMetadata> result = new ArrayList<>(toUpload.size());

        LatchedActionListener<IndexMetadata> latchedActionListener = new LatchedActionListener<>(ActionListener.wrap((IndexMetadata t) -> {
            logger.trace(String.format(Locale.ROOT, "IndexMetadata uploaded successfully for %s", t.getIndex().toString()));
        }, ex -> {
            assert ex instanceof IndexMetadataTransferException;
            logger.error(
                () -> new ParameterizedMessage(
                    "Exception during transfer of IndexMetadata to Remote {}",
                    ((IndexMetadataTransferException) ex).getIndexMetadata().getIndex().toString()
                ),
                ex
            );
            exceptionList.add(ex);
        }), latch);

        for (IndexMetadata indexMetadata : toUpload) {
            final UploadedIndexMetadata uploadedIndexMetadata = writeIndexMetadata(clusterState, latchedActionListener, indexMetadata);
            result.add(uploadedIndexMetadata);
        }

        try {
            if (latch.await(20000, TimeUnit.MILLISECONDS) == false) {
                TimeoutException ex = new TimeoutException("Timed out waiting for transfer of index metadata to complete");
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            Thread.currentThread().interrupt();
            throw ex;
        }

        return result;
    }

    private UploadedIndexMetadata writeIndexMetadata(
        ClusterState clusterState,
        LatchedActionListener<IndexMetadata> latchedActionListener,
        IndexMetadata indexMetadata
    ) throws IOException {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            indexMetadata.getIndexUUID()
        );

        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(indexMetadata),
            ex -> latchedActionListener.onFailure(new IndexMetadataTransferException(indexMetadata, ex))
        );
        INDEX_METADATA_FORMAT.writeAsync(
            indexMetadata,
            indexMetadataContainer,
            indexMetadataFileName(indexMetadata),
            blobStoreRepository.getCompressor(),
            completionListener
        );
        final String indexMetadataKey = indexMetadataContainer.path().buildAsString() + indexMetadataFileName(indexMetadata);
        return new UploadedIndexMetadata(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID(), indexMetadataKey);
    }

    @Nullable
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

    @Override
    public void close() throws IOException {
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
    }

    public boolean ensureRepositorySet() {
        if (blobStoreRepository != null) {
            return true;
        }
        final String remoteStoreRepo = REMOTE_CLUSTER_STATE_REPOSITORY_SETTING.get(settings);
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        try {
            final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
            assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
            blobStoreRepository = (BlobStoreRepository) repository;
        } catch (RepositoryMissingException e) {
            logger.error("Repository {} is not initialized", remoteStoreRepo);
            return false;
        }
        return true;
    }

    private ClusterMetadataMarker uploadMarker(
        ClusterState clusterState,
        List<UploadedIndexMetadata> uploadedIndexMetadata,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String markerFileName = getMarkerFileName(clusterState.term(), clusterState.version());
            final ClusterMetadataMarker marker = new ClusterMetadataMarker(
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                Version.CURRENT,
                nodeId,
                committed,
                uploadedIndexMetadata
            );
            writeMetadataMarker(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), marker, markerFileName);
            return marker;
        }
    }

    private String writeIndexMetadata(String clusterName, String clusterUUID, IndexMetadata uploadIndexMetadata, String fileName)
        throws IOException {
        final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
        final BlobContainer indexMetadataContainer = indexMetadataContainer(clusterName, clusterUUID, uploadIndexMetadata.getIndexUUID());
        INDEX_METADATA_FORMAT.write(uploadIndexMetadata, indexMetadataContainer, fileName, blobStoreRepository.getCompressor());
        final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
        logger.info("Writing index metadata for {} took {} ms", uploadIndexMetadata.getIndex().getName(), durationMillis);
        // returning full path
        return indexMetadataContainer.path().buildAsString() + fileName;
    }

    private void writeMetadataMarker(String clusterName, String clusterUUID, ClusterMetadataMarker uploadMarker, String fileName)
        throws IOException {
        final long startTimeMillis = relativeTimeMillisSupplier.getAsLong();
        final BlobContainer metadataMarkerContainer = markerContainer(clusterName, clusterUUID);
        CLUSTER_METADATA_MARKER_FORMAT.write(uploadMarker, metadataMarkerContainer, fileName, blobStoreRepository.getCompressor());
        final long durationMillis = relativeTimeMillisSupplier.getAsLong() - startTimeMillis;
        logger.info("Writing metadata marker took {} ms", durationMillis);
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

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
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

    class IndexMetadataTransferException extends RuntimeException {

        private final IndexMetadata indexMetadata;

        public IndexMetadataTransferException(IndexMetadata indexMetadata, Throwable cause) {
            super(cause);
            this.indexMetadata = indexMetadata;
        }

        public IndexMetadata getIndexMetadata() {
            return indexMetadata;
        }
    }
}
