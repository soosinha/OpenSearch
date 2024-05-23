/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.PersistedClusterStateService.SLOW_WRITE_LOGGING_THRESHOLD;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_BLOCKS;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTE;
import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.DISCOVERY_NODES;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteReadResult;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.UploadedMetadataResults;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.clusterUUIDContainer;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.COORDINATION_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_DELIMITER;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.CUSTOM_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.SETTING_METADATA;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.TEMPLATES_METADATA;
import static org.opensearch.gateway.remote.RemoteIndexMetadata.INDEX_PATH_TOKEN;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.remote.RemoteRoutingTableService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
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

    public static final TimeValue REMOTE_STATE_READ_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> REMOTE_STATE_READ_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.read_timeout",
        REMOTE_STATE_READ_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private TimeValue remoteStateReadTimeout;
    private final String nodeId;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final LongSupplier relativeTimeNanosSupplier;
    private final ThreadPool threadpool;
    private final List<IndexMetadataUploadListener> indexMetadataUploadListeners;
    private BlobStoreRepository blobStoreRepository;
    private RemoteRoutingTableService remoteRoutingTableService;
    private volatile TimeValue slowWriteLoggingThreshold;

    private final RemotePersistenceStats remoteStateStats;
    private RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private RemoteClusterStateAttributesManager remoteClusterStateAttributesManager;
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private BlobStoreTransferService blobStoreTransferService;
    private final String CLUSTER_STATE_UPLOAD_TIME_LOG_STRING = "writing cluster state for version [{}] took [{}ms]";
    private final String METADATA_UPDATE_LOG_STRING = "wrote metadata for [{}] indices and skipped [{}] unchanged "
        + "indices, coordination metadata updated : [{}], settings metadata updated : [{}], templates metadata "
        + "updated : [{}], custom metadata updated : [{}]";
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;

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
        ThreadPool threadPool,
        List<IndexMetadataUploadListener> indexMetadataUploadListeners
    ) {
        assert isRemoteStoreClusterStateEnabled(settings) : "Remote cluster state is not enabled";
        logger.info("REMOTE STATE ENABLED");

        this.nodeId = nodeId;
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.relativeTimeNanosSupplier = relativeTimeNanosSupplier;
        this.threadpool = threadPool;
        clusterSettings = clusterService.getClusterSettings();
        this.slowWriteLoggingThreshold = clusterSettings.get(SLOW_WRITE_LOGGING_THRESHOLD);
        clusterSettings.addSettingsUpdateConsumer(SLOW_WRITE_LOGGING_THRESHOLD, this::setSlowWriteLoggingThreshold);
        this.remoteStateReadTimeout = clusterSettings.get(REMOTE_STATE_READ_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STATE_READ_TIMEOUT_SETTING, this::setRemoteClusterStateEnabled);
        this.remoteStateStats = new RemotePersistenceStats();

        if(isRemoteRoutingTableEnabled(settings)) {
            this.remoteRoutingTableService = new RemoteRoutingTableService(repositoriesService,
                settings, threadPool);
            logger.info("REMOTE ROUTING ENABLED");
        } else {
            logger.info("REMOTE ROUTING DISABLED");
        }
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
            true,
            new ArrayList<>(clusterState.getRoutingTable().indicesRouting().values()));

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
            uploadedMetadataResults.uploadedIndicesRoutingMetadata,
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
        final Map<String, Metadata.Custom> customsToUpload = remoteGlobalMetadataManager.getUpdatedCustoms(clusterState, previousClusterState);
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

        List<IndexRoutingTable> indicesRoutingToUpload = new ArrayList<>();
        if(remoteRoutingTableService!=null) {
            indicesRoutingToUpload = remoteRoutingTableService.getChangedIndicesRouting(previousClusterState, clusterState);
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


        uploadedMetadataResults = writeMetadataInParallel(
            clusterState,
            toUpload,
            prevIndexMetadataByName,
            firstUploadForSplitGlobalMetadata ? clusterState.metadata().customs() : customsToUpload,
            updateCoordinationMetadata,
            updateSettingsMetadata,
            updateTemplatesMetadata,
            updateDiscoveryNodes,
            updateClusterBlocks,
            indicesRoutingToUpload
            );


        // update the map if the metadata was uploaded
        uploadedMetadataResults.uploadedIndexMetadata.forEach(
            uploadedIndexMetadata -> allUploadedIndexMetadata.put(uploadedIndexMetadata.getIndexName(), uploadedIndexMetadata)
        );
        allUploadedCustomMap.putAll(uploadedMetadataResults.uploadedCustomMetadataMap);
        // remove the data for removed custom/indices
        customsToBeDeletedFromRemote.keySet().forEach(allUploadedCustomMap::remove);
        indicesToBeDeletedFromRemote.keySet().forEach(allUploadedIndexMetadata::remove);

        List<ClusterMetadataManifest.UploadedIndexMetadata> allUploadedIndicesRouting = new ArrayList<>();
        if(remoteRoutingTableService!=null) {
            allUploadedIndicesRouting = remoteRoutingTableService.getAllUploadedIndicesRouting(previousManifest, uploadedMetadataResults.uploadedIndicesRoutingMetadata, indicesToBeDeletedFromRemote.keySet());
        }

        final ClusterMetadataManifest manifest = remoteManifestManager.uploadManifest(
            clusterState,
            new ArrayList<>(allUploadedIndexMetadata.values()),
            previousManifest.getPreviousClusterUUID(),
            updateCoordinationMetadata ? uploadedMetadataResults.uploadedCoordinationMetadata : previousManifest.getCoordinationMetadata(),
            updateSettingsMetadata ? uploadedMetadataResults.uploadedSettingsMetadata : previousManifest.getSettingsMetadata(),
            updateTemplatesMetadata ? uploadedMetadataResults.uploadedTemplatesMetadata : previousManifest.getTemplatesMetadata(),
            firstUploadForSplitGlobalMetadata || !customsToUpload.isEmpty()
                ? allUploadedCustomMap
                : previousManifest.getCustomMetadataMap(),
            firstUploadForSplitGlobalMetadata || updateDiscoveryNodes ? uploadedMetadataResults.uploadedDiscoveryNodes : previousManifest.getDiscoveryNodesMetadata(),
            firstUploadForSplitGlobalMetadata || updateClusterBlocks ? uploadedMetadataResults.uploadedClusterBlocks : previousManifest.getClusterBlocksMetadata(),
            new ClusterStateDiffManifest(clusterState, previousClusterState),
            allUploadedIndicesRouting, false
        );

        logger.info("MANIFEST IN INC STATE {}", manifest);

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
        boolean uploadClusterBlock,
        List<IndexRoutingTable> indicesRoutingToUpload) throws IOException {
        int totalUploadTasks = indexToUpload.size() + customToUpload.size() + (uploadCoordinationMetadata ? 1 : 0) + (uploadSettingsMetadata
            ? 1 : 0) + (uploadTemplateMetadata ? 1 : 0) + (uploadDiscoveryNodes  ? 1 : 0) + (uploadClusterBlock ? 1 : 0) + indicesRoutingToUpload.size();
        CountDownLatch latch = new CountDownLatch(totalUploadTasks);
        Map<String, CheckedRunnable<IOException>> uploadTasks = new HashMap<>(totalUploadTasks);
        Map<String, ClusterMetadataManifest.UploadedMetadata> results = new HashMap<>(totalUploadTasks);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalUploadTasks));

        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> listener = new LatchedActionListener<>(
            ActionListener.wrap((ClusterMetadataManifest.UploadedMetadata uploadedMetadata) -> {
                logger.info(String.format(Locale.ROOT, "Metadata component %s uploaded successfully.", uploadedMetadata.getComponent()));
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
                    clusterState.metadata().persistentSettings(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    listener,
                    null
                )
            );
        }
        if (uploadCoordinationMetadata) {
            uploadTasks.put(
                COORDINATION_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState.metadata().coordinationMetadata(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    listener,
                    null
                )
            );
        }
        if (uploadTemplateMetadata) {
            uploadTasks.put(
                TEMPLATES_METADATA,
                remoteGlobalMetadataManager.getAsyncMetadataWriteAction(
                    clusterState.metadata().templatesMetadata(),
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    listener,
                    null
                )
            );
        }
        if (uploadDiscoveryNodes) {
            uploadTasks.put(
                DISCOVERY_NODES,
                remoteClusterStateAttributesManager.getAsyncMetadataWriteAction(
                    clusterState,
                    DISCOVERY_NODES,
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
                    value,
                    clusterState.metadata().version(),
                    clusterState.metadata().clusterUUID(),
                    listener,
                    key
                )
            );
        });
        indexToUpload.forEach(indexMetadata -> {
            uploadTasks.put(
                indexMetadata.getIndexName(),
                remoteIndexMetadataManager.getIndexMetadataAsyncAction(indexMetadata, clusterState.metadata().clusterUUID(), listener)
            );
        });

        indicesRoutingToUpload.forEach(indexRoutingTable -> {
            try {
                uploadTasks.put(
                    indexRoutingTable.getIndex().getName() + "--indexRouting",
                    remoteRoutingTableService.getIndexRoutingAsyncAction(clusterState, indexRoutingTable, listener)
                );
            } catch (IOException e) {
                e.printStackTrace();
            }
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
            if (uploadedMetadata.getClass().equals(UploadedIndexMetadata.class) &&
                uploadedMetadata.getComponent().contains(RemoteRoutingTableService.INDEX_ROUTING_METADATA_PREFIX)) {
                response.uploadedIndicesRoutingMetadata.add((UploadedIndexMetadata) uploadedMetadata);
            } else if (name.contains(CUSTOM_METADATA)) {
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
        logger.info("response {}", response.uploadedIndicesRoutingMetadata.toString());
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

    private ActionListener<Void> getIndexMetadataUploadActionListener(
        List<IndexMetadata> newIndexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        CountDownLatch latch,
        List<Exception> exceptionList,
        String listenerName
    ) {
        long startTime = System.nanoTime();
        return new LatchedActionListener<>(
            ActionListener.wrap(
                ignored -> logger.trace(
                    new ParameterizedMessage(
                        "listener={} : Invoked successfully with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                        listenerName,
                        newIndexMetadataList,
                        prevIndexMetadataByName.values(),
                        (System.nanoTime() - startTime)
                    )
                ),
                ex -> {
                    logger.error(
                        new ParameterizedMessage(
                            "listener={} : Exception during invocation with indexMetadataList={} prevIndexMetadataList={} tookTimeNs={}",
                            listenerName,
                            newIndexMetadataList,
                            prevIndexMetadataByName.values(),
                            (System.nanoTime() - startTime)
                        ),
                        ex
                    );
                    exceptionList.add(ex);
                }
            ),
            latch
        );
    }

    public RemoteManifestManager getRemoteManifestManager() {
        return remoteManifestManager;
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
        String clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(blobStoreRepository, clusterSettings,threadpool, getBlobStoreTransferService(), clusterName);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(blobStoreRepository, clusterSettings,threadpool, clusterName, getBlobStoreTransferService());
        remoteClusterStateAttributesManager = new RemoteClusterStateAttributesManager(getBlobStoreTransferService(), blobStoreRepository, threadpool, clusterName);
        remoteManifestManager = new RemoteManifestManager(blobStoreRepository, clusterSettings, nodeId);
        remoteClusterStateCleanupManager.start();
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

    BlobStoreRepository getBlobStoreRepository() {
        return blobStoreRepository;
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
    public ClusterState getLatestClusterState(String clusterName, String clusterUUID) throws IOException {
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

    private ClusterState readClusterStateInParallel(
        ClusterState previousState,
        ClusterMetadataManifest manifest,
        String clusterName,
        String clusterUUID,
        String localNodeId,
        List<UploadedIndexMetadata> indicesToRead,
        Map<String, UploadedMetadataAttribute> customToRead,
        boolean readCoordinationMetadata,
        boolean readSettingsMetadata,
        boolean readTemplatesMetadata,
        boolean readDiscoveryNodes,
        boolean readClusterBlocks,
        List<UploadedIndexMetadata> indicesRoutingToRead
    ) throws IOException {
        int totalReadTasks = indicesToRead.size() + customToRead.size() + indicesRoutingToRead.size() + (readCoordinationMetadata ? 1 : 0) + (readSettingsMetadata ? 1 : 0) + (readTemplatesMetadata ? 1 : 0) + (readDiscoveryNodes ? 1 : 0) + (readClusterBlocks ? 1 : 0);
        CountDownLatch latch = new CountDownLatch(totalReadTasks);
        List<CheckedRunnable<IOException>> asyncMetadataReadActions = new ArrayList<>();
        List<RemoteReadResult> readResults = new ArrayList<>();
        List<RemoteRoutingTableService.RemoteIndexRoutingResult> readIndexRoutingTableResults = new ArrayList<>();
        List<Exception> indexRoutingExceptionList = Collections.synchronizedList(new ArrayList<>(indicesRoutingToRead.size()));
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(totalReadTasks));

        LatchedActionListener<RemoteClusterStateUtils.RemoteReadResult> listener = new LatchedActionListener<>(
            ActionListener.wrap(
                response -> {
                    logger.info("Successfully read cluster state component from remote");
                    readResults.add(response);
                },
                ex -> {
                    logger.error("Failed to read cluster state from remote", ex);
                    exceptionList.add(ex);
                }
            ),
            latch
        );

        for (UploadedIndexMetadata indexMetadata : indicesToRead) {
            asyncMetadataReadActions.add(
                remoteIndexMetadataManager.getAsyncIndexMetadataReadAction(
                    clusterUUID,
                    indexMetadata.getUploadedFilename(),
                    listener
                )
            );
        }

        LatchedActionListener<RemoteRoutingTableService.RemoteIndexRoutingResult> routingTableLatchedActionListener = new LatchedActionListener<>(
            ActionListener.wrap(
                response -> {
                    logger.info("Successfully read cluster state component from remote");
                    readIndexRoutingTableResults.add(response);
                },
                ex -> {
                    logger.error("Failed to read cluster state from remote", ex);
                    indexRoutingExceptionList.add(ex);
                }
            ),
            latch
        );

        for (UploadedIndexMetadata indexRouting: indicesRoutingToRead) {
            asyncMetadataReadActions.add(
                remoteRoutingTableService.getAsyncIndexMetadataReadAction(
                    clusterName,
                    clusterUUID,
                    indexRouting.getUploadedFilename(),
                    new Index(indexRouting.getIndexName(), indexRouting.getIndexUUID()),
                    routingTableLatchedActionListener
                )
            );
        }

        for (Map.Entry<String, UploadedMetadataAttribute> entry : customToRead.entrySet()) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    clusterName,
                    clusterUUID,
                    CUSTOM_METADATA,
                    entry.getKey(),
                    entry.getValue().getUploadedFilename(),
                    listener
                )
            );
        }

        if (readCoordinationMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    clusterName,
                    clusterUUID,
                    COORDINATION_METADATA,
                    COORDINATION_METADATA,
                    manifest.getCoordinationMetadata().getUploadedFilename(),
                    listener
                )
            );
        }

        if (readSettingsMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    clusterName,
                    clusterUUID,
                    SETTING_METADATA,
                    SETTING_METADATA,
                    manifest.getSettingsMetadata().getUploadedFilename(),
                    listener
                )
            );
        }

        if (readTemplatesMetadata) {
            asyncMetadataReadActions.add(
                remoteGlobalMetadataManager.getAsyncMetadataReadAction(
                    clusterName,
                    clusterUUID,
                    TEMPLATES_METADATA,
                    TEMPLATES_METADATA,
                    manifest.getTemplatesMetadata().getUploadedFilename(),
                    listener
                )
            );
        }

        if (readDiscoveryNodes) {
            asyncMetadataReadActions.add(
                remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
                    clusterUUID,
                    DISCOVERY_NODES,
                    manifest.getDiscoveryNodesMetadata().getUploadedFilename(),
                    listener
                )
            );
        }

        if (readClusterBlocks) {
            asyncMetadataReadActions.add(
                remoteClusterStateAttributesManager.getAsyncMetadataReadAction(
                    clusterUUID,
                    CLUSTER_BLOCKS,
                    manifest.getClusterBlocksMetadata().getUploadedFilename(),
                    listener
                )
            );
        }

        for (CheckedRunnable<IOException> asyncMetadataReadAction : asyncMetadataReadActions) {
            asyncMetadataReadAction.run();
        }

        try {
            if (latch.await(this.remoteStateReadTimeout.getMillis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException exception = new RemoteStateTransferException(
                    "Timed out waiting to read cluster state from remote within timeout " + this.remoteStateReadTimeout
                );
                exceptionList.forEach(exception::addSuppressed);
                throw exception;
            }
        } catch (InterruptedException e) {
            exceptionList.forEach(e::addSuppressed);
            RemoteStateTransferException ex = new RemoteStateTransferException("Interrupted while waiting to read cluster state from metadata");
            Thread.currentThread().interrupt();
            throw ex;
        }

        if (!exceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException("Exception during reading cluster state from remote");
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }

        if (!indexRoutingExceptionList.isEmpty()) {
            RemoteStateTransferException exception = new RemoteStateTransferException("Exception during reading routing table from remote");
            exceptionList.forEach(exception::addSuppressed);
            throw exception;
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(previousState);
        AtomicReference<DiscoveryNodes.Builder> discoveryNodesBuilder = new AtomicReference<>(DiscoveryNodes.builder());
        Metadata.Builder metadataBuilder = Metadata.builder(previousState.metadata());
        metadataBuilder.clusterUUID(manifest.getClusterUUID());
        metadataBuilder.clusterUUIDCommitted(manifest.isClusterUUIDCommitted());
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        Map<String, IndexRoutingTable> indicesRouting = new HashMap<>();

        readResults.forEach(remoteReadResult -> {
            switch (remoteReadResult.getComponent()) {
                case INDEX_PATH_TOKEN:
                    IndexMetadata indexMetadata = (IndexMetadata) remoteReadResult.getObj();
                    indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
                    break;
                case CUSTOM_METADATA:
                    metadataBuilder.putCustom(remoteReadResult.getComponentName(), (Metadata.Custom) remoteReadResult.getObj());
                    break;
                case COORDINATION_METADATA:
                    metadataBuilder.coordinationMetadata((CoordinationMetadata) remoteReadResult.getObj());
                    break;
                case SETTING_METADATA:
                    metadataBuilder.persistentSettings((Settings) remoteReadResult.getObj());
                    break;
                case TEMPLATES_METADATA:
                    metadataBuilder.templates((TemplatesMetadata) remoteReadResult.getObj());
                    break;
                case CLUSTER_STATE_ATTRIBUTE:
                    if (remoteReadResult.getComponentName().equals(DISCOVERY_NODES)) {
                        discoveryNodesBuilder.set(DiscoveryNodes.builder((DiscoveryNodes) remoteReadResult.getObj()));
                    } else if (remoteReadResult.getComponentName().equals(CLUSTER_BLOCKS)) {
                        clusterStateBuilder.blocks((ClusterBlocks) remoteReadResult.getObj());
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown component: " + remoteReadResult.getComponent());
            }
        });

        readIndexRoutingTableResults.forEach(remoteIndexRoutingResult -> {
            indicesRouting.put(remoteIndexRoutingResult.getIndexName(), remoteIndexRoutingResult.getIndexRoutingTable());
        });

        metadataBuilder.indices(indexMetadataMap);
        if (readDiscoveryNodes) {
            clusterStateBuilder.nodes(discoveryNodesBuilder.get().localNodeId(localNodeId));
        }
        return clusterStateBuilder.metadata(metadataBuilder)
            .version(manifest.getStateVersion())
            .stateUUID(manifest.getStateUUID())
            .routingTable(new RoutingTable(manifest.getRoutingTableVersion(), indicesRouting))
            .build();
    }

    public ClusterState getClusterStateForManifest(String clusterName, ClusterMetadataManifest manifest, String localNodeId) throws IOException {
        // todo make this async
        return readClusterStateInParallel(
            ClusterState.EMPTY_STATE,
            manifest,
            clusterName,
            manifest.getClusterUUID(),
            localNodeId,
            manifest.getIndices(),
            manifest.getCustomMetadataMap(),
            manifest.getCoordinationMetadata() != null,
            manifest.getSettingsMetadata() != null,
            manifest.getTemplatesMetadata() != null,
            manifest.getDiscoveryNodesMetadata() != null,
            manifest.getClusterBlocksMetadata() != null,
            manifest.getIndicesRouting()
        );
    }

    public ClusterState getClusterStateUsingDiff(String clusterName, ClusterMetadataManifest manifest, ClusterState previousState, String localNodeId) throws IOException {
        assert manifest.getDiffManifest() != null;
        ClusterStateDiffManifest diff = manifest.getDiffManifest();
        List<UploadedIndexMetadata> updatedIndices = diff.getIndicesUpdated().stream().map(idx -> {
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndices().stream().filter(idx2 -> idx2.getIndexName().equals(idx)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());

        List<UploadedIndexMetadata> updatedIndexRouting = diff.getIndicesRoutingUpdated().stream().map(idx -> {
            Optional<UploadedIndexMetadata> uploadedIndexMetadataOptional = manifest.getIndicesRouting().stream().filter(idx2 -> idx2.getIndexName().equals(idx)).findFirst();
            assert uploadedIndexMetadataOptional.isPresent() == true;
            return uploadedIndexMetadataOptional.get();
        }).collect(Collectors.toList());

        Map<String, UploadedMetadataAttribute> updatedCustomMetadata = new HashMap<>();
        if (diff.getCustomMetadataUpdated() != null) {
            for (String customType : diff.getCustomMetadataUpdated().keySet()) {
                updatedCustomMetadata.put(customType, manifest.getCustomMetadataMap().get(customType));
            }
        }
        ClusterState updatedClusterState = readClusterStateInParallel(
            previousState,
            manifest,
            clusterName,
            manifest.getClusterUUID(),
            localNodeId,
            updatedIndices,
            updatedCustomMetadata,
            diff.isCoordinationMetadataUpdated(),
            diff.isSettingsMetadataUpdated(),
            diff.isTemplatesMetadataUpdated(),
            diff.isDiscoveryNodesUpdated(),
            diff.isClusterBlocksUpdated(),
            updatedIndexRouting
        );
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(updatedClusterState);
        Metadata.Builder metadataBuilder = Metadata.builder(updatedClusterState.metadata());
        for (String index:diff.getIndicesDeleted()) {
            metadataBuilder.remove(index);
        }
        if (diff.getCustomMetadataUpdated() != null) {
            for (String customType : diff.getCustomMetadataUpdated().keySet()) {
                Metadata.Custom custom = remoteGlobalMetadataManager.getCustomsMetadata(manifest.getClusterUUID(),
                    manifest.getCustomMetadataMap().get(customType).getUploadedFilename(), customType);
                metadataBuilder.putCustom(customType, custom);
            }
        }
        if (diff.getCustomMetadataDeleted() != null) {
            for (String customType : diff.getCustomMetadataDeleted()) {
                metadataBuilder.removeCustom(customType);
            }
        }

        HashMap<String, IndexRoutingTable> indexRoutingTables = new HashMap<>(updatedClusterState.getRoutingTable().getIndicesRouting());

        for(String indexName: diff.getIndicesRoutingDeleted()){
            indexRoutingTables.remove(indexName);
        }

        RoutingTable routingTable = new RoutingTable(manifest.getRoutingTableVersion(), indexRoutingTables);

        return clusterStateBuilder.
            stateUUID(manifest.getStateUUID()).
            version(manifest.getStateVersion()).
            metadata(metadataBuilder).
            routingTable(routingTable).
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

    public void setRemoteClusterStateEnabled(TimeValue remoteStateReadTimeout) {
        this.remoteStateReadTimeout = remoteStateReadTimeout;
    }


    public RemoteClusterStateCleanupManager getCleanupManager() {
        return remoteClusterStateCleanupManager;
    }

    private BlobStoreTransferService getBlobStoreTransferService() {
        if (blobStoreTransferService == null) {
            blobStoreTransferService = new BlobStoreTransferService(getBlobStore(), threadpool);
        }
        return blobStoreTransferService;
    }

    Set<String> getAllClusterUUIDs(String clusterName) throws IOException {
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
            .collect(Collectors.toMap(UploadedIndexMetadata::getIndexName, Function.identity()));
        for (UploadedIndexMetadata uploadedIndexMetadata : first.getIndices()) {
            final IndexMetadata firstIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                uploadedIndexMetadata,
                first.getClusterUUID(),
                first.getCodecVersion()
            );
            final UploadedIndexMetadata secondUploadedIndexMetadata = secondIndices.get(uploadedIndexMetadata.getIndexName());
            if (secondUploadedIndexMetadata == null) {
                return false;
            }
            final IndexMetadata secondIndexMetadata = remoteIndexMetadataManager.getIndexMetadata(
                secondUploadedIndexMetadata,
                second.getClusterUUID(),
                second.getCodecVersion()
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
