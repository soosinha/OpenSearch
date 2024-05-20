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
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.UploadedMetadataResults;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.clusterUUIDContainer;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTINGS_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTING_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteIndexMetadataManager.INDEX_METADATA_FORMAT;
import static org.opensearch.gateway.remote.RemoteIndexMetadataManager.INDEX_PATH_TOKEN;
import static org.opensearch.gateway.remote.RemoteManifestManager.MANIFEST_PATH_TOKEN;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * A Service which provides APIs to upload and download cluster metadata from remote store.
 *
 * @opensearch.internal
 */
public class RemoteClusterStateService implements Closeable {
    public static final int RETAINED_MANIFESTS = 10;
    public static final int SKIP_CLEANUP_STATE_CHANGES = 10;

    private static final Logger logger = LogManager.getLogger(RemoteClusterStateService.class);

    /**
     * Used to specify if cluster state metadata should be published to remote store
     */
    public static final Setting<Boolean> REMOTE_CLUSTER_STATE_ENABLED_SETTING = Setting.boolSetting(
        "cluster.remote_store.state.enabled",
        false,
        Property.NodeScope,
        Property.Final
    );

    /**
     * Setting to specify the interval to do run stale file cleanup job
     */
    public static final Setting<TimeValue> REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.cleanup_interval",
        TimeValue.timeValueMinutes(5),
        TimeValue.timeValueMillis(-1),
        Property.NodeScope,
        Property.Dynamic
    );

    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private RemoteRoutingTableService remoteRoutingTableService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private final AtomicBoolean deleteStaleMetadataRunning = new AtomicBoolean(false);
    private final RemotePersistenceStats remoteStateStats;
    private RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private TimeValue staleFileCleanupInterval;
    private AsyncStaleFileDeletion staleFileDeletionTask;
    private final String CLUSTER_STATE_UPLOAD_TIME_LOG_STRING = "writing cluster state for version [{}] took [{}ms]";
    private final String METADATA_UPDATE_LOG_STRING = "wrote metadata for [{}] indices and skipped [{}] unchanged "
        + "indices, coordination metadata updated : [{}], settings metadata updated : [{}], templates metadata "
        + "updated : [{}], custom metadata updated : [{}]";
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;
    public static final int MANIFEST_CURRENT_CODEC_VERSION = ClusterMetadataManifest.CODEC_V2;
    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 2;

    // ToXContent Params with gateway mode.
    // We are using gateway context mode to persist all custom metadata.
    public static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(1);
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }
    private String latestClusterName;
    private String latestClusterUUID;
    private long lastCleanupAttemptState;
    private boolean isClusterManagerNode;

    public RemoteClusterStateService(
        String nodeId,
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterService clusterService,
        LongSupplier relativeTimeNanosSupplier,
        ThreadPool threadPool
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        logger.info("REMOTE STATE ENABLED");

        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.remoteStateStats = new RemotePersistenceStats();

        if(isRemoteRoutingTableEnabled(settings)) {
            this.remoteRoutingTableService = new RemoteRoutingTableService(repositoriesService,
                settings, clusterSettings);
            logger.info("REMOTE ROUTING ENABLED");
        } else {
            logger.info("REMOTE ROUTING DISABLED");
        }
        this.staleFileCleanupInterval = clusterSettings.get(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_CLUSTER_STATE_CLEANUP_INTERVAL_SETTING, this::updateCleanupInterval);
        this.lastCleanupAttemptState = 0;
        this.isClusterManagerNode = DiscoveryNode.isClusterManagerNode(settings);
        this.remoteClusterStateCleanupManager = new RemoteClusterStateCleanupManager(this, clusterService);
        this.indexMetadataUploadListeners = indexMetadataUploadListeners;
    }

    /**
     * This method uploads entire cluster state metadata to the configured blob store. For now only index metadata upload is supported. This method should be
     * invoked by the elected cluster manager when the remote cluster state is enabled.
     *
     * @return A manifest object which contains the details of uploaded entity metadata.
     */
    @Nullable
    public ClusterMetadataManifest writeFullMetadata(ClusterState clusterState, String previousClusterUUID) throws IOException {
        logger.info("WRITING FULL STATE");
        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }


        UploadedMetadataResults uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            new ArrayList<>(clusterState.metadata().indices().values()),
            Collections.emptyMap(),
            clusterState.metadata().customs(),
            true,
            true,
            true,
            true,
            true
        );

        List<UploadedIndexMetadata> routingIndexMetadata = new ArrayList<>();
        if(remoteRoutingTableService!=null) {
            routingIndexMetadata = remoteRoutingTableService.writeFullRoutingTable(clusterState, previousClusterUUID);
            logger.info("routingIndexMetadata {}", routingIndexMetadata);
        }

        final ClusterMetadataManifest manifest = remoteManifestManager.uploadManifest(
            clusterState,
            uploadedMetadataResults.uploadedIndexMetadata,
            previousClusterUUID,
            uploadedMetadataResults.uploadedCoordinationMetadata,
            uploadedMetadataResults.uploadedSettingsMetadata,
            uploadedMetadataResults.uploadedTemplatesMetadata,
            uploadedMetadataResults.uploadedCustomMetadataMap,
            uploadedMetadataResults.uploadedDiscoveryNodes,
            uploadedMetadataResults.uploadedClusterBlocks,
            new ClusterStateDiffManifest(clusterState, ClusterState.EMPTY_STATE),
            routingIndexMetadata,
            false
        );

        logger.info("MANIFEST IN FULL STATE {}", manifest);
        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; " + "wrote full state with [{}] indices",
                durationMillis,
                slowWriteLoggingThreshold,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        } else {
            logger.info(
                "writing cluster state took [{}ms]; " + "wrote full state with [{}] indices and global metadata",
                durationMillis,
                uploadedMetadataResults.uploadedIndexMetadata.size()
            );
        }
        return manifest;
    }

    /**
     * This method uploads the diff between the previous cluster state and the current cluster state. The previous manifest file is needed to create the new
     * manifest. The new manifest file is created by using the unchanged metadata from the previous manifest and the new metadata changes from the current
     * cluster state.
     *
     * @return The uploaded ClusterMetadataManifest file
     */
    @Nullable
    public ClusterMetadataManifest writeIncrementalMetadata(
        ClusterState previousClusterState,
        ClusterState clusterState,
        ClusterMetadataManifest previousManifest
    ) throws IOException {
        logger.info("WRITING INCREMENTAL STATE");

        final long startTimeNanos = relativeTimeNanosSupplier.getAsLong();
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousClusterState.metadata().coordinationMetadata().term() == clusterState.metadata().coordinationMetadata().term();

        final Map<String, UploadedMetadataAttribute> customsToBeDeletedFromRemote = new HashMap<>(previousManifest.getCustomMetadataMap());
        final Map<String, Metadata.Custom> customsToUpload = getUpdatedCustoms(clusterState, previousClusterState);
        final Map<String, UploadedMetadataAttribute> allUploadedCustomMap = new HashMap<>(previousManifest.getCustomMetadataMap());
        for (final String custom : clusterState.metadata().customs().keySet()) {
            // remove all the customs which are present currently
            customsToBeDeletedFromRemote.remove(custom);
        }
        final Map<String, IndexMetadata> indicesToBeDeletedFromRemote = new HashMap<>(previousClusterState.metadata().indices());
        int numIndicesUpdated = 0;
        int numIndicesUnchanged = 0;
        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));

        List<IndexMetadata> toUpload = new ArrayList<>();
        // We prepare a map that contains the previous index metadata for the indexes for which version has changed.
        Map<String, IndexMetadata> prevIndexMetadataByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            String indexName = indexMetadata.getIndex().getName();
            final IndexMetadata prevIndexMetadata = indicesToBeDeletedFromRemote.get(indexName);
            Long previousVersion = prevIndexMetadata != null ? prevIndexMetadata.getVersion() : null;
            if (previousVersion == null || indexMetadata.getVersion() != previousVersion) {
                logger.debug(
                    "updating metadata for [{}], changing version from [{}] to [{}]",
                    indexMetadata.getIndex(),
                    previousVersion,
                    indexMetadata.getVersion()
                );
                numIndicesUpdated++;
                toUpload.add(indexMetadata);
                prevIndexMetadataByName.put(indexName, prevIndexMetadata);
            } else {
                numIndicesUnchanged++;
            }
            // index present in current cluster state
            indicesToBeDeletedFromRemote.remove(indexMetadata.getIndex().getName());
        }
        UploadedMetadataResults uploadedMetadataResults;
        // For migration case from codec V0 or V1 to V2, we have added null check on metadata attribute files,
        // If file is empty and codec is 1 then write global metadata.
        boolean firstUploadForSplitGlobalMetadata = !previousManifest.hasMetadataAttributesFiles();
        boolean updateCoordinationMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isCoordinationMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        ;
        boolean updateSettingsMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isSettingsMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        boolean updateTemplatesMetadata = firstUploadForSplitGlobalMetadata
            || Metadata.isTemplatesMetadataEqual(previousClusterState.metadata(), clusterState.metadata()) == false;
        // Write Global Metadata
        final boolean updateGlobalMetadata = Metadata.isGlobalStateEquals(
            previousClusterState.metadata(),
            clusterState.metadata()
        ) == false;
        // ToDo: check if these needs to be updated or not
        final boolean updateDiscoveryNodes = clusterState.getNodes().delta(previousClusterState.getNodes()).hasChanges();
        final boolean updateClusterBlocks = clusterState.blocks().equals(previousClusterState.blocks());

        // Write Index Metadata
        final Map<String, IndexMetadata> previousStateIndexMetadataByName = new HashMap<>();
        for (final IndexMetadata indexMetadata : previousClusterState.metadata().indices().values()) {
            previousStateIndexMetadataByName.put(indexMetadata.getIndex().getName(), indexMetadata);
        }

        final Map<String, ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndexMetadata = previousManifest.getIndices()
            .stream()
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));


        uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            toUpload,
            prevIndexMetadataByName,
            firstUploadForSplitGlobalMetadata ? clusterState.metadata().customs() : customsToUpload,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            updateDiscoveryNodes,
            updateClusterBlocks
        );

        List<UploadedIndexMetadata> routingIndexMetadata = new ArrayList<>();
        if(remoteRoutingTableService!=null) {
            routingIndexMetadata = remoteRoutingTableService.writeIncrementalRoutingTable(previousClusterState, clusterState, previousManifest);
            logger.info("routingIndexMetadata incremental {}", routingIndexMetadata);
        }

        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        // remove the data for removed custom/indices
        customsToBeDeletedFromRemote.keySet().forEach(allUploadedCustomMap::remove);
        indicesToBeDeletedFromRemote.keySet().forEach(allUploadedIndexMetadata::remove);

        final ClusterMetadataManifest manifest = uploadManifest(
            clusterState,
            new ArrayList<>(allUploadedIndexMetadata.values()),
            previousManifest.getPreviousClusterUUID(),
            updateCoordinationMetadata ? uploadedMetadataResults.uploadedCoordinationMetadata : previousManifest.getCoordinationMetadata(),
            updateSettingsMetadata ? uploadedMetadataResults.uploadedSettingsMetadata : previousManifest.getSettingsMetadata(),
            updateTemplatesMetadata ? uploadedMetadataResults.uploadedTemplatesMetadata : previousManifest.getTemplatesMetadata(),
            firstUploadForSplitGlobalMetadata || !customsToUpload.isEmpty()
                ? allUploadedCustomMap
                : previousManifest.getCustomMetadataMap(),
            firstUpload || updateDiscoveryNodes ? uploadedMetadataResults.uploadedDiscoveryNodes : previousManifest.getDiscoveryNodesMetadata(),
            firstUpload || updateClusterBlocks ? uploadedMetadataResults.uploadedClusterBlocks : previousManifest.getClusterBlocksMetadata(),
            new ClusterStateDiffManifest(clusterState, previousClusterState),
            routingIndexMetadata, false
        );
        this.latestClusterName = clusterState.getClusterName().value();
        this.latestClusterUUID = clusterState.metadata().clusterUUID();

        final long durationMillis = TimeValue.nsecToMSec(relativeTimeNanosSupplier.getAsLong() - startTimeNanos);
        remoteStateStats.stateSucceeded();
        remoteStateStats.stateTook(durationMillis);
        ParameterizedMessage clusterStateUploadTimeMessage = new ParameterizedMessage(
            CLUSTER_STATE_UPLOAD_TIME_LOG_STRING,
            manifest.getStateVersion(),
            durationMillis
        );
        ParameterizedMessage metadataUpdateMessage = new ParameterizedMessage(
            METADATA_UPDATE_LOG_STRING,
            numIndicesUpdated,
            numIndicesUnchanged,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            customsToUpload.size()
        );
        if (durationMillis >= slowWriteLoggingThreshold.getMillis()) {
            logger.warn(
                "{} which is above the warn threshold of [{}]; {}",
                clusterStateUploadTimeMessage,
                "writing cluster state took [{}ms] which is above the warn threshold of [{}]; "
                    + "wrote  metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                durationMillis,
                slowWriteLoggingThreshold,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsToUpload.size()
            );
        } else {
            logger.info("{}; {}", clusterStateUploadTimeMessage, metadataUpdateMessage);
            logger.info(
                "writing cluster state for version [{}] took [{}ms]; "
                    + "wrote metadata for [{}] indices and skipped [{}] unchanged indices, coordination metadata updated : [{}], "
                    + "settings metadata updated : [{}], templates metadata updated : [{}], custom metadata updated : [{}]",
                manifest.getStateVersion(),
                durationMillis,
                numIndicesUpdated,
                numIndicesUnchanged,
                updateCoordinationMetadata,
                updateSettingsMetadata,
                updateTemplatesMetadata,
                customsToUpload.size()
            );
        }
        return manifest;
    }

    private UploadedMetadataResults writeMetadataInParallel(
        ClusterState clusterState,
        List<IndexMetadata> indexToUpload,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        Map<String, Metadata.Custom> customToUpload,
        boolean uploadCoordinationMetadata,
        boolean uploadSettingsMetadata,
        boolean uploadTemplateMetadata,
        boolean uploadDiscoveryNodes,
        boolean uploadClusterBlock
    ) throws IOException {
        assert Objects.nonNull(indexMetadataUploadListeners) : "indexMetadataUploadListeners can not be null";
        int totalUploadTasks = indexToUpload.size() + customToUpload.size() + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata
            ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0) + (uploadDiscoveryNodes  ? 1 : 0) + (uploadClusterBlock ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        Map<String, CheckedRunnable<IOException>> uploadTasks = new HashMap<>(totalUploadTasks);
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new HashMap<>(totalUploadTasks);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(
            ActionListener.wrap((ClusterMetadataManifest.UploadedMetadata uploadedMetadata) -> {
                logger.trace(String.format(Locale.ROOT, "Metadata component %s uploaded successfully.", uploadedMetadata.getComponent()));
                results.put(uploadedMetadata.getComponent(), uploadedMetadata);
            }, ex -> {
                logger.error(
                    () -> new ParameterizedMessage("Exception during transfer of Metadata Fragment to Remote {}", ex.getMessage()),
                    ex
                );
                exceptionList.add(ex);
            }),
            latch
        );

        if (uploadSettingsMetadata) {
            uploadTasks.put(
                SETTING_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    SETTING_METADATA,
                    SETTINGS_METADATA_FORMAT,
                    clusterState.metadata().persistentSettings(),
                    listener
                )
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.put(
                COORDINATION_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    COORDINATION_METADATA,
                    COORDINATION_METADATA_FORMAT,
                    clusterState.metadata().coordinationMetadata(),
                    listener
                )
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.put(
                TEMPLATES_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    TEMPLATES_METADATA,
                    TEMPLATES_METADATA_FORMAT,
                    clusterState.metadata().templatesMetadata(),
                    listener
                )
            );
        }
        if (uploadDiscoveryNodes) {
            uploadTasks.put(
                DISCOVERY_NODES,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    clusterState,
                    DISCOVERY_NODES,
                    DISCOVERY_NODES_FORMAT,
                    clusterState.nodes(),
                    listener
                )
            );
        }
        if (uploadClusterBlock) {
            uploadTasks.put(
                CLUSTER_BLOCKS,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    clusterState,
                    CLUSTER_BLOCKS,
                    CLUSTER_BLOCKS_FORMAT,
                    clusterState.blocks(),
                    listener
                )
            );
        }
        customToUpload.forEach((key, value) -> {
            String customComponent = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, key);
            uploadTasks.put(
                customComponent,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState,
                    customComponent,
                    CUSTOM_METADATA_FORMAT,
                    value,
                    listener
                )
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.put(
                indexMetadata.getIndexName(),
                remoteIndexMetadataManager.getIndexMetadataAsyncAction(clusterState, indexMetadata, listener)
            );
        });

        // start async upload of all required metadata files
        for (CheckedRunnable<IOException> uploadTask : uploadTasks.values()) {
            uploadTask.run();
        }
        invokeIndexMetadataUploadListeners(indexToUpload, prevIndexMetadataByName, latch, exceptionList);

        try {
            if (latch.await(remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                // TODO: We should add metrics where transfer is timing out. [Issue: #10687]
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(
                        Locale.ROOT,
                        "Timed out waiting for transfer of following metadata to complete - %s",
                        String.join(", ", uploadTasks.keySet())
                    )
                );
                exceptionList.forEach(ex::addSuppressed);
                throw ex;
            }
        } catch (InterruptedException ex) {
            exceptionList.forEach(ex::addSuppressed);
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Timed out waiting for transfer of metadata to complete - %s",
                    String.join(", ", uploadTasks.keySet())
                ),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(
                    Locale.ROOT,
                    "Exception during transfer of following metadata to Remote - %s",
                    String.join(", ", uploadTasks.keySet())
                )
            );
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        UploadedMetadataResults response = new UploadedMetadataResults();
        results.forEach((name, uploadedMetadata) -> {
            if (name.contains(CUSTOM_METADATA)) {
                // component name for custom metadata will look like custom--<metadata-attribute>
                String custom = name.split(DELIMITER)[0].split(CUSTOM_DELIMITER)[1];
                response.uploadedCustomMetadataMap.put(
                    custom,
                    new UploadedMetadataAttribute(custom, uploadedMetadata.getUploadedFilename())
                );
            } else if (COORDINATION_METADATA.equals(name)) {
                response.uploadedCoordinationMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (SETTING_METADATA.equals(name)) {
                response.uploadedSettingsMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (TEMPLATES_METADATA.equals(name)) {
                response.uploadedTemplatesMetadata = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (name.contains(UploadedIndexMetadata.COMPONENT_PREFIX)) {
                response.uploadedIndexMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (DISCOVERY_NODES.equals(uploadedMetadata.getComponent())) {
                response.uploadedDiscoveryNodes = (UploadedMetadataAttribute) uploadedMetadata;
            } else if (CLUSTER_BLOCKS.equals(uploadedMetadata.getComponent())) {
                response.uploadedClusterBlocks = (UploadedMetadataAttribute) uploadedMetadata;
            } else {
                throw new IllegalStateException("Unexpected metadata component " + uploadedMetadata.getComponent());
            }
        });
        return response;
    }

    /**
     * Invokes the index metadata upload listener but does not wait for the execution to complete.
     */
    private void invokeIndexMetadataUploadListeners(
        List<IndexMetadata> updatedIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList
    ) {
        for (IndexMetadataUploadListener listener : indexMetadataUploadListeners) {
            String listenerName = listener.getClass().getSimpleName();
            listener.onUpload(
                updatedIndexMetadataList,
                prevIndexMetadataByName,
                getIndexMetadataUploadActionListener(updatedIndexMetadataList, prevIndexMetadataByName, latch, exceptionList, listenerName)
            );
        }

    }


    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param clusterState          current ClusterState
     * @param indexMetadata         {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    private void writeIndexMetadataAsync(
        ClusterState clusterState,
        IndexMetadata indexMetadata,
        LatchedActionListener<UploadedIndexMetadata> latchedActionListener
    ) throws IOException {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            indexMetadata.getIndexUUID()
        );
        final String indexMetadataFilename = indexMetadataFileName(indexMetadata);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataContainer.path().buildAsString() + indexMetadataFilename
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().toString(), ex))
        );

        INDEX_METADATA_FORMAT.writeAsyncWithUrgentPriority(
            indexMetadata,
            indexMetadataContainer,
            indexMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );
    }

    @Nullable
    public ClusterMetadataManifest markLastStateAsCommitted(ClusterState clusterState, ClusterMetadataManifest previousManifest)
        throws IOException {
        assert clusterState != null : "Last accepted cluster state is not set";
        if (clusterState.nodes().isLocalNodeElectedClusterManager() == false) {
            logger.error("Local node is not elected cluster manager. Exiting");
            return null;
        }
        assert previousManifest != null : "Last cluster metadata manifest is not set";
        ClusterMetadataManifest committedManifest = remoteManifestManager.uploadManifest(
            clusterState,
            previousManifest.getIndices(),
            previousManifest.getPreviousClusterUUID(),
            previousManifest.getCoordinationMetadata(),
            previousManifest.getSettingsMetadata(),
            previousManifest.getTemplatesMetadata(),
            previousManifest.getCustomMetadataMap(),
            previousManifest.getDiscoveryNodesMetadata(),
            previousManifest.getClusterBlocksMetadata(),
            previousManifest.getDiffManifest(),
            previousManifest.getIndicesRouting(), true
        );
        if (!previousManifest.isClusterUUIDCommitted() && committedManifest.isClusterUUIDCommitted()) {
            remoteClusterStateCleanupManager.deleteStaleClusterUUIDs(clusterState, committedManifest);
        }

        return committedManifest;
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        return remoteManifestManager.getLatestClusterMetadataManifest(clusterName, clusterUUID);
    }

    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(String clusterName, String clusterUUID, long term, long version) {
        return remoteManifestManager.getClusterMetadataManifestByTermVersion(clusterName, clusterUUID, term, version);
    }

    @Override
    public void close() throws IOException {
        remoteClusterStateCleanupManager.close();
        if (blobStoreRepository != null) {
            IOUtils.close(blobStoreRepository);
        }
        if(this.remoteRoutingTableService != null) {
            this.remoteRoutingTableService.close();
        }
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Cluster State repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
        if(this.remoteRoutingTableService != null) {
            this.remoteRoutingTableService.start();
        }
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(blobStoreRepository, clusterSettings);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(blobStoreRepository, clusterSettings);
        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(blobStoreRepository);
        remoteManifestManager = new RemoteManifestManager(blobStoreRepository, clusterSettings, nodeId);
        remoteClusterStateCleanupManager.start();
    }

    private String fetchPreviousClusterUUID(String clusterName, String clusterUUID) {
        final Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (!latestManifest.isPresent()) {
            final String previousClusterUUID = getLastKnownUUIDFromRemote(clusterName);
            assert !clusterUUID.equals(previousClusterUUID) : "Last cluster UUID is same current cluster UUID";
            return previousClusterUUID;
        }
        return latestManifest.get().getPreviousClusterUUID();
    }

    private void setSlowWriteLoggingThreshold(TimeValue slowWriteLoggingThreshold) {
        this.slowWriteLoggingThreshold = slowWriteLoggingThreshold;
    }

    //Package private for unit test
    RemoteRoutingTableService getRemoteRoutingTableService() {
        return this.remoteRoutingTableService;
    }

    ThreadPool getThreadpool() {
        return threadpool;
    }

    BlobStore getBlobStore() {
        return blobStoreRepository.blobStore();
    }

    /**
     * Fetch latest ClusterState from remote, including global metadata, index metadata and cluster state version
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return {@link IndexMetadata}
     */
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID) {
        start();
        Optional<ClusterMetadataManifest> clusterMetadataManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            clusterName,
            clusterUUID
        );
        if (clusterMetadataManifest.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Latest cluster metadata manifest is not present for the provided clusterUUID: %s", clusterUUID)
            );
        }

        return getClusterStateForManifest(clusterName, clusterMetadataManifest.get(), nodeId);
    }

    public ClusterState getClusterStateForManifest(String clusterName, ClusterMetadataManifest manifest, String localNodeId) {
        // todo make this async
        // Fetch Global Metadata
        Metadata globalMetadata = remoteGlobalMetadataManager.getGlobalMetadata(clusterName, manifest.getClusterUUID(), manifest);

        // Fetch Index Metadata
        Map<String, IndexMetadata> indices = remoteIndexMetadataManager.getIndexMetadataMap(
            clusterName,
            manifest.getClusterUUID(),
            manifest
        );
        DiscoveryNodes discoveryNodes = (DiscoveryNodes) remoteClusterStateAttributesManager.readMetadata(DISCOVERY_NODES_FORMAT, clusterName, manifest.getClusterUUID(), manifest.getDiscoveryNodesMetadata().getUploadedFilename());

        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        indices.values().forEach(indexMetadata -> { indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata); });

        if(remoteRoutingTableService != null) {
            logger.info(" The Remote Routing Table is fetching the Full Routing Table");
            RoutingTable routingTable = remoteRoutingTableService.getFullRoutingTable(manifest.getRoutingTableVersion(), manifest.getIndicesRouting());
            return ClusterState.builder(ClusterState.EMPTY_STATE)
                .version(manifest.getStateVersion())
                .stateUUID(manifest.getStateUUID())
                .nodes(DiscoveryNodes.builder(discoveryNodes).localNodeId(localNodeId))
                .metadata(Metadata.builder(globalMetadata).indices(indexMetadataMap).build())
                .routingTable(routingTable)
                .build();
        }

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .version(manifest.getStateVersion())
            .stateUUID(manifest.getStateUUID())
            .nodes(DiscoveryNodes.builder(discoveryNodes).localNodeId(localNodeId))
            .metadata(Metadata.builder(globalMetadata).indices(indexMetadataMap).build())
            .build();
    }

    public ClusterState getClusterStateUsingDiff(String clusterName, ClusterMetadataManifest manifest, ClusterState previousState, String localNodeId) {
        assert manifest.getDiffManifest() != null;
        ClusterStateDiffManifest diff = manifest.getDiffManifest();
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(previousState);
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());

        for (String index:diff.getIndicesUpdated()) {
            //todo optimize below iteration
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndices().stream().filter(idx -> idx.getIndexName().equals(index)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            IndexMetadata indexMetadata = remoteIndexMetadataManager.getIndexMetadata(clusterName, manifest.getClusterUUID(), uploadedIndexMetadataOptional.get());
            metadataBuilder.put(indexMetadata, false);
        }
        for (String index:diff.getIndicesDeleted()) {
            metadataBuilder.remove(index);
        }
        if (diff.isCoordinationMetadataUpdated()) {
            CoordinationMetadata coordinationMetadata = remoteGlobalMetadataManager.getCoordinationMetadata(clusterName, manifest.getClusterUUID(), manifest.getCoordinationMetadata().getUploadedFilename());
            metadataBuilder.coordinationMetadata(coordinationMetadata);
        }
        if (diff.isSettingsMetadataUpdated()) {
            Settings settings = remoteGlobalMetadataManager.getSettingsMetadata(clusterName, manifest.getClusterUUID(), manifest.getSettingsMetadata().getUploadedFilename());
            metadataBuilder.persistentSettings(settings);
        }
        if (diff.isTemplatesMetadataUpdated()) {
            TemplatesMetadata templatesMetadata = remoteGlobalMetadataManager.getTemplatesMetadata(clusterName, manifest.getClusterUUID(), manifest.getTemplatesMetadata().getUploadedFilename());
            metadataBuilder.templates(templatesMetadata);
        }
        if (diff.isDiscoveryNodesUpdated()) {
            DiscoveryNodes discoveryNodes = (DiscoveryNodes) remoteClusterStateAttributesManager.readMetadata(DISCOVERY_NODES_FORMAT, clusterName, manifest.getClusterUUID(), manifest.getDiscoveryNodesMetadata().getUploadedFilename());
            clusterStateBuilder.nodes(DiscoveryNodes.builder(discoveryNodes).localNodeId(localNodeId));
        }
        if (diff.getCustomMetadataUpdated() != null) {
            for (String customType : diff.getCustomMetadataUpdated().keySet()) {
                Metadata.Custom custom = remoteGlobalMetadataManager.getCustomsMetadata(clusterName, manifest.getClusterUUID(),
                    manifest.getCustomMetadataMap().get(customType).getUploadedFilename(), customType);
                metadataBuilder.putCustom(customType, custom);
            }
        }

        // Constructing Routing Table
        if(remoteRoutingTableService!= null){
            RoutingTable routingTable = remoteRoutingTableService.getIncrementalRoutingTable(previousState, manifest);
            return clusterStateBuilder.
                stateUUID(manifest.getStateUUID()).
                version(manifest.getStateVersion()).
                metadata(metadataBuilder).
                routingTable(routingTable).
                build();
        }


        return clusterStateBuilder.
            stateUUID(manifest.getStateUUID()).
            version(manifest.getStateVersion()).
            metadata(metadataBuilder).
            build();
    }

    /**
     * Fetch the previous cluster UUIDs from remote state store and return the most recent valid cluster UUID
     *
     * @param clusterName The cluster name for which previous cluster UUID is to be fetched
     * @return Last valid cluster UUID
     */
    public String getLastKnownUUIDFromRemote(String clusterName) {
        try {
            Set<String> clusterUUIDs = getAllClusterUUIDs(clusterName);
            Map<String, ClusterMetadataManifest> latestManifests = remoteManifestManager.getLatestManifestForAllClusterUUIDs(
                clusterName,
                clusterUUIDs
            );
            List<String> validChain = createClusterChain(latestManifests, clusterName);
            if (validChain.isEmpty()) {
                return ClusterState.UNKNOWN_UUID;
            }
            return validChain.get(0);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while fetching previous UUIDs from remote store for cluster name: %s", clusterName),
                e
            );
        }
    }

    private Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
        Map<String, BlobContainer> clusterUUIDMetadata = clusterUUIDContainer(blobStoreRepository, clusterName).children();
        if (clusterUUIDMetadata == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(clusterUUIDMetadata.keySet());
    }

    /**
     * This method creates a valid cluster UUID chain.
     *
     * @param manifestsByClusterUUID Map of latest ClusterMetadataManifest for every cluster UUID
     * @return List of cluster UUIDs. The first element is the most recent cluster UUID in the chain
     */
    private List<String> createClusterChain(final Map<String, ClusterMetadataManifest> manifestsByClusterUUID, final String clusterName) {
        final List<ClusterMetadataManifest> validClusterManifests = manifestsByClusterUUID.values()
            .stream()
            .filter(this::isValidClusterUUID)
            .collect(Collectors.toList());
        final Map<String, String> clusterUUIDGraph = validClusterManifests.stream()
            .collect(Collectors.toMap(ClusterMetadataManifest::getClusterUUID, ClusterMetadataManifest::getPreviousClusterUUID));
        final List<String> topLevelClusterUUIDs = validClusterManifests.stream()
            .map(ClusterMetadataManifest::getClusterUUID)
            .filter(clusterUUID -> !clusterUUIDGraph.containsValue(clusterUUID))
            .collect(Collectors.toList());

        if (topLevelClusterUUIDs.isEmpty()) {
            // This can occur only when there are no valid cluster UUIDs
            assert validClusterManifests.isEmpty() : "There are no top level cluster UUIDs even when there are valid cluster UUIDs";
            logger.info("There is no valid previous cluster UUID. All cluster UUIDs evaluated are: {}", manifestsByClusterUUID.keySet());
            return Collections.emptyList();
        }
        if (topLevelClusterUUIDs.size() > 1) {
            logger.info("Top level cluster UUIDs: {}", topLevelClusterUUIDs);
            // If the valid cluster UUIDs are more that 1, it means there was some race condition where
            // more then 2 cluster manager nodes tried to become active cluster manager and published
            // 2 cluster UUIDs which followed the same previous UUID.
            final Map<String, ClusterMetadataManifest> manifestsByClusterUUIDTrimmed = trimClusterUUIDs(
                manifestsByClusterUUID,
                topLevelClusterUUIDs,
                clusterName
            );
            if (manifestsByClusterUUID.size() == manifestsByClusterUUIDTrimmed.size()) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "The system has ended into multiple valid cluster states in the remote store. "
                            + "Please check their latest manifest to decide which one you want to keep. Valid Cluster UUIDs: - %s",
                        topLevelClusterUUIDs
                    )
                );
            }
            return createClusterChain(manifestsByClusterUUIDTrimmed, clusterName);
        }
        final List<String> validChain = new ArrayList<>();
        String currentUUID = topLevelClusterUUIDs.get(0);
        while (currentUUID != null && !ClusterState.UNKNOWN_UUID.equals(currentUUID)) {
            validChain.add(currentUUID);
            // Getting the previous cluster UUID of a cluster UUID from the clusterUUID Graph
            currentUUID = clusterUUIDGraph.get(currentUUID);
        }
        logger.info("Known UUIDs found in remote store : [{}]", validChain);
        return validChain;
    }

    /**
     * This method take a map of manifests for different cluster UUIDs and removes the
     * manifest of a cluster UUID if the latest metadata for that cluster UUID is equivalent
     * to the latest metadata of its previous UUID.
     * @return Trimmed map of manifests
     */
    private Map<String, ClusterMetadataManifest> trimClusterUUIDs(
        final Map<String, ClusterMetadataManifest> latestManifestsByClusterUUID,
        final List<String> validClusterUUIDs,
        final String clusterName
    ) {
        final Map<String, ClusterMetadataManifest> trimmedUUIDs = new HashMap<>(latestManifestsByClusterUUID);
        for (String clusterUUID : validClusterUUIDs) {
            ClusterMetadataManifest currentManifest = trimmedUUIDs.get(clusterUUID);
            // Here we compare the manifest of current UUID to that of previous UUID
            // In case currentUUID's latest manifest is same as previous UUIDs latest manifest,
            // that means it was restored from previousUUID and no IndexMetadata update was performed on it.
            if (!ClusterState.UNKNOWN_UUID.equals(currentManifest.getPreviousClusterUUID())) {
                ClusterMetadataManifest previousManifest = trimmedUUIDs.get(currentManifest.getPreviousClusterUUID());
                if (isMetadataEqual(currentManifest, previousManifest, clusterName)
                    && remoteGlobalMetadataManager.isGlobalMetadataEqual(currentManifest, previousManifest, clusterName)) {
                    trimmedUUIDs.remove(clusterUUID);
                }
            }
        }
        return trimmedUUIDs;
    }

    private boolean isMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        // todo clusterName can be set as final in the constructor
        if (first.getIndices().size() != second.getIndices().size()) {
            return false;
        }
        final Map<String, UploadedIndexMetadata> secondIndices = second.getIndices()
            .stream()
            .collect(Collectors.toMap(md -> md.getIndexName(), Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                clusterName,
                first.getClusterUUID(),
                uploadedIndexMetadata
            );
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                clusterName,
                second.getClusterUUID(),
                secondUploadedIndexMetadata
            );
            if (firstIndexMetadata.equals(secondIndexMetadata) == false) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidClusterUUID(ClusterMetadataManifest manifest) {
        return manifest.isClusterUUIDCommitted();
    }

    public void writeMetadataFailed() {
        getStats().stateFailed();
    }

    /**
     * Exception for Remote state transfer.
     */
    public static class RemoteStateTransferException extends RuntimeException {

        public RemoteStateTransferException(String errorDesc) {
            super(errorDesc);
        }

        public RemoteStateTransferException(String errorDesc, Throwable cause) {
            super(errorDesc, cause);
        }
    }

    public RemotePersistenceStats getStats() {
        return remoteStateStats;
    }
}
