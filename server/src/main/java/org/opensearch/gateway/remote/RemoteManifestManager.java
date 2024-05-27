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
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;

public class RemoteManifestManager {

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final BlobStoreTransferService blobStoreTransferService;
    private final BlobStoreRepository blobStoreRepository;
    private volatile TimeValue metadataManifestUploadTimeout;
    private final String nodeId;
    private final ThreadPool threadPool;
    private static final Logger logger = LogManager.getLogger(RemoteManifestManager.class);

    RemoteManifestManager(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, String nodeId, ThreadPool threadPool) {
        this.blobStoreTransferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        this.nodeId = nodeId;
        this.threadPool = threadPool;
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
    }

    ClusterMetadataManifest uploadManifest(
        ClusterState clusterState,
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadata,
        String previousClusterUUID,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedCoordinationMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedSettingsMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedTransientSettingsMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedTemplatesMetadata,
        Map<String, ClusterMetadataManifest.UploadedMetadataAttribute> uploadedCustomMetadataMap,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedDiscoveryNodesMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedClusterBlocksMetadata,
        ClusterStateDiffManifest clusterDiffManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> routingIndexMetadata, boolean committed
    ) {
        synchronized (this) {
            ClusterMetadataManifest.Builder manifestBuilder = ClusterMetadataManifest.builder();
            manifestBuilder.clusterTerm(clusterState.term())
                .stateVersion(clusterState.getVersion())
                .clusterUUID(clusterState.metadata().clusterUUID())
                .stateUUID(clusterState.stateUUID())
                .opensearchVersion(Version.CURRENT)
                .nodeId(nodeId)
                .committed(committed)
                .codecVersion(RemoteClusterMetadataManifest.MANIFEST_CURRENT_CODEC_VERSION)
                .indices(uploadedIndexMetadata)
                .previousClusterUUID(previousClusterUUID)
                .clusterUUIDCommitted(clusterState.metadata().clusterUUIDCommitted())
                .coordinationMetadata(uploadedCoordinationMetadata)
                .settingMetadata(uploadedSettingsMetadata)
                .templatesMetadata(uploadedTemplatesMetadata)
                .customMetadataMap(uploadedCustomMetadataMap)
                .discoveryNodesMetadata(uploadedDiscoveryNodesMetadata)
                .clusterBlocksMetadata(uploadedClusterBlocksMetadata)
                .diffManifest(clusterDiffManifest)
                .routingTableVersion(clusterState.getRoutingTable().version())
                .indicesRouting(routingIndexMetadata)
                .metadataVersion(clusterState.metadata().version())
                .transientSettingsMetadata(uploadedTransientSettingsMetadata);
            final ClusterMetadataManifest manifest = manifestBuilder.build();
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest);
            return manifest;
        }
    }

    private void writeMetadataManifest(String clusterName, String clusterUUID, ClusterMetadataManifest uploadManifest) {
        AtomicReference<String> result = new AtomicReference<String>();
        AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();

        // latch to wait until upload is not finished
        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener completionListener = new LatchedActionListener<>(ActionListener.wrap(resp -> {
            logger.trace(String.format(Locale.ROOT, "Manifest file uploaded successfully."));
        }, ex -> { exceptionReference.set(ex); }), latch);

        RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(uploadManifest, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        remoteClusterMetadataManifest.writeAsync(completionListener);

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete")
                );
                throw ex;
            }
        } catch (InterruptedException ex) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete - %s"),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionReference.get() != null) {
            throw new RemoteStateTransferException(exceptionReference.get().getMessage(), exceptionReference.get());
        }
        logger.debug(
            "Metadata manifest file [{}] written during [{}] phase. ",
            remoteClusterMetadataManifest.getBlobFileName(),
            uploadManifest.isCommitted() ? "commit" : "publish"
        );
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    /**
     * Fetch the cluster metadata manifest using term and version
     * @param clusterName uuid of cluster state to refer to in remote
     * @param clusterUUID name of the cluster
     * @param term election term of the cluster
     * @param version cluster state version number
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(String clusterName, String clusterUUID, long term, long version) {
        Optional<String> manifestFileName = getManifestFileNameByTermVersion(clusterName, clusterUUID, term, version);
        return manifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            String fullBlobName = getManifestFolderPath(clusterName, clusterUUID).buildAsString() + filename;
            RemoteClusterMetadataManifest remoteClusterMetadataManifest = new RemoteClusterMetadataManifest(fullBlobName, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
            return remoteClusterMetadataManifest.read();
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID).add(RemoteClusterMetadataManifest.MANIFEST_PATH_TOKEN);
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return this.metadataManifestUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue newMetadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = newMetadataManifestUploadTimeout;
    }

    /**
     * Fetch ClusterMetadataManifest files from remote state store in order
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param limit max no of files to fetch
     * @return all manifest file names
     */
    private List<BlobMetadata> getManifestFileNames(String clusterName, String clusterUUID, String filePrefix, int limit) throws IllegalStateException {
        try {

            /*
              {@link BlobContainer#listBlobsByPrefixInSortedOrder} will list the latest manifest file first
              as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
              when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            return manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                filePrefix,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
    }

    static String getManifestFilePrefixForTermVersion(long term, long version) {
        return String.join(
            DELIMITER,
            RemoteClusterMetadataManifest.MANIFEST_FILE_PREFIX,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version)
        ) + DELIMITER;
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, RemoteClusterMetadataManifest.MANIFEST_FILE_PREFIX + DELIMITER, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }

    private Optional<String> getManifestFileNameByTermVersion(String clusterName, String clusterUUID, long term, long version) throws IllegalStateException {
        final String filePrefix = getManifestFilePrefixForTermVersion(term, version);
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, filePrefix, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}, term: {}, version: {}", clusterName, clusterUUID, term, version);
        return Optional.empty();
    }
}
