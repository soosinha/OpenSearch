/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static java.util.Objects.requireNonNull;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

public class RemoteGlobalMetadataManager {

    public static final String GLOBAL_METADATA_PATH_TOKEN = "global-metadata";
    public static final String COORDINATION_METADATA = "coordination";
    public static final String SETTING_METADATA = "settings";
    public static final String TEMPLATES_METADATA = "templates";
    public static final String CUSTOM_METADATA = "custom";
    public static final String CUSTOM_DELIMITER = "--";

    public static final TimeValue GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.global_metadata.upload_timeout",
        GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "metadata",
        METADATA_NAME_FORMAT,
        Metadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<CoordinationMetadata> COORDINATION_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "coordination",
        METADATA_NAME_FORMAT,
        CoordinationMetadata::fromXContent
    );

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "settings",
        METADATA_NAME_FORMAT,
        Settings::fromXContent
    );

    public static final ChecksumBlobStoreFormat<TemplatesMetadata> TEMPLATES_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "templates",
        METADATA_NAME_FORMAT,
        TemplatesMetadata::fromXContent
    );

    public static final int GLOBAL_METADATA_CURRENT_CODEC_VERSION = 1;

    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

    private volatile TimeValue globalMetadataUploadTimeout;
    private final BlobStoreTransferService blobStoreTransferService;
    private final String clusterName;

    RemoteGlobalMetadataManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, ThreadPool threadPool, BlobStoreTransferService blobStoreTransferService,
        String clusterName) {
        this.blobStoreRepository = blobStoreRepository;
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.threadPool = threadPool;
        this.blobStoreTransferService = blobStoreTransferService;
        this.clusterName = clusterName;
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setGlobalMetadataUploadTimeout);
    }

    /**
     * Allows async upload of Metadata components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        Object objectToUpload,
        long metadataVersion,
        String clusterUUID,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener,
        String customType
    ) {
        AbstractRemoteBlobStoreObject remoteBlobStoreObject = getRemoteObject(objectToUpload, metadataVersion, clusterUUID, customType);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteBlobStoreObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException("Upload failed", ex))
        );
        return remoteBlobStoreObject.writeAsync(completionListener);
    }

    CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterName,
        String clusterUUID,
        String component,
        String componentName,
        String uploadFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        AbstractRemoteBlobStoreObject remoteBlobStoreObject;
        if (component.equals(COORDINATION_METADATA)) {
            remoteBlobStoreObject = new RemoteCoordinationMetadata(uploadFilename, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else if (component.equals(TEMPLATES_METADATA)) {
            remoteBlobStoreObject = new RemoteTemplatesMetadata(uploadFilename, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else if (component.equals(SETTING_METADATA)) {
            remoteBlobStoreObject = new RemotePersistentSettingsMetadata(uploadFilename, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else if (component.equals(CUSTOM_METADATA)) {
            remoteBlobStoreObject = new RemoteCustomMetadata(uploadFilename, componentName, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else {
            throw new RemoteStateTransferException("Unknown component " + componentName);
        }
        ActionListener actionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, component, componentName)), listener::onFailure);
        return () -> remoteBlobStoreObject.readAsync(actionListener);
    }

    Metadata getGlobalMetadata(String clusterName, String clusterUUID, ClusterMetadataManifest clusterMetadataManifest) {
        String globalMetadataFileName = clusterMetadataManifest.getGlobalMetadataFileName();
        try {
            // Fetch Global metadata
            if (globalMetadataFileName != null) {
                String[] splitPath = globalMetadataFileName.split("/");
                return GLOBAL_METADATA_FORMAT.read(
                    globalMetadataContainer(clusterName, clusterUUID),
                    splitPath[splitPath.length - 1],
                    blobStoreRepository.getNamedXContentRegistry()
                );
            } else if (clusterMetadataManifest.hasMetadataAttributesFiles()) {
                CoordinationMetadata coordinationMetadata = getCoordinationMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getCoordinationMetadata().getUploadedFilename()
                );
                Settings settingsMetadata = getSettingsMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getSettingsMetadata().getUploadedFilename()
                );
                TemplatesMetadata templatesMetadata = getTemplatesMetadata(
                    clusterUUID,
                    clusterMetadataManifest.getTemplatesMetadata().getUploadedFilename()
                );
                Metadata.Builder builder = new Metadata.Builder();
                builder.coordinationMetadata(coordinationMetadata);
                builder.persistentSettings(settingsMetadata);
                builder.templates(templatesMetadata);
                builder.clusterUUID(clusterMetadataManifest.getClusterUUID());
                builder.clusterUUIDCommitted(clusterMetadataManifest.isClusterUUIDCommitted());
                //todo add metadata version
                //builder.version(clusterMetadataManifest.met)
                clusterMetadataManifest.getCustomMetadataMap()
                    .forEach(
                        (key, value) -> builder.putCustom(
                            key,
                            getCustomsMetadata(clusterUUID, value.getUploadedFilename(), key)
                        )
                    );
                return builder.build();
            } else {
                return Metadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Global Metadata - %s", globalMetadataFileName),
                e
            );
        }
    }

    public CoordinationMetadata getCoordinationMetadata(String clusterUUID, String coordinationMetadataFileName) {
        try {
            // Fetch Coordination metadata
            if (coordinationMetadataFileName != null) {
                RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata(coordinationMetadataFileName, clusterUUID,
                    blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
                return remoteCoordinationMetadata.read();
            } else {
                return CoordinationMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Coordination Metadata - %s", coordinationMetadataFileName),
                e
            );
        }
    }

    public Settings getSettingsMetadata(String clusterUUID, String settingsMetadataFileName) {
        try {
            // Fetch Settings metadata
            if (settingsMetadataFileName != null) {
                RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata(settingsMetadataFileName, clusterUUID,
                    blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
                return remotePersistentSettingsMetadata.read();
            } else {
                return Settings.EMPTY;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Settings Metadata - %s", settingsMetadataFileName),
                e
            );
        }
    }

    public TemplatesMetadata getTemplatesMetadata(String clusterUUID, String templatesMetadataFileName) {
        try {
            // Fetch Templates metadata
            if (templatesMetadataFileName != null) {
                RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata(templatesMetadataFileName, clusterUUID,
                    blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
                return remoteTemplatesMetadata.read();
            } else {
                return TemplatesMetadata.EMPTY_METADATA;
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Templates Metadata - %s", templatesMetadataFileName),
                e
            );
        }
    }

    public Metadata.Custom getCustomsMetadata(String clusterUUID, String customMetadataFileName, String custom) {
        requireNonNull(customMetadataFileName);
        try {
            // Fetch Custom metadata
            RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata(customMetadataFileName, custom, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
            return remoteCustomMetadata.read();
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading Custom Metadata - %s", customMetadataFileName),
                e
            );
        }
    }

    Map<String, Metadata.Custom> getUpdatedCustoms(ClusterState currentState, ClusterState previousState) {
        if (Metadata.isCustomMetadataEqual(previousState.metadata(), currentState.metadata())) {
            return new HashMap<>();
        }
        Map<String, Metadata.Custom> updatedCustom = new HashMap<>();
        Set<String> currentCustoms = new HashSet<>(currentState.metadata().customs().keySet());
        for (Map.Entry<String, Metadata.Custom> cursor : previousState.metadata().customs().entrySet()) {
            if (cursor.getValue().context().contains(Metadata.XContentContext.GATEWAY)) {
                if (currentCustoms.contains(cursor.getKey())
                    && !cursor.getValue().equals(currentState.metadata().custom(cursor.getKey()))) {
                    // If the custom metadata is updated, we need to upload the new version.
                    updatedCustom.put(cursor.getKey(), currentState.metadata().custom(cursor.getKey()));
                }
                currentCustoms.remove(cursor.getKey());
            }
        }
        for (String custom : currentCustoms) {
            Metadata.Custom cursor = currentState.metadata().custom(custom);
            if (cursor.context().contains(Metadata.XContentContext.GATEWAY)) {
                updatedCustom.put(custom, cursor);
            }
        }
        return updatedCustom;
    }

    private AbstractRemoteBlobStoreObject getRemoteObject(Object object, long metadataVersion, String clusterUUID, String customType) {
        if (object instanceof CoordinationMetadata) {
            return new RemoteCoordinationMetadata((CoordinationMetadata) object, metadataVersion, clusterUUID, blobStoreTransferService,
                blobStoreRepository, clusterName, threadPool);
        } else if (object instanceof  Settings) {
            return new RemotePersistentSettingsMetadata((Settings) object, metadataVersion, clusterUUID, blobStoreTransferService,
                blobStoreRepository, clusterName, threadPool);
        } else if (object instanceof TemplatesMetadata) {
            return new RemoteTemplatesMetadata((TemplatesMetadata) object, metadataVersion, clusterUUID, blobStoreTransferService,
                blobStoreRepository, clusterName, threadPool);
        } else if (object instanceof Custom) {
            return new RemoteCustomMetadata((Custom) object, customType ,metadataVersion, clusterUUID, blobStoreTransferService,
                blobStoreRepository, clusterName, threadPool);
        }
        throw new RemoteStateTransferException("Remote object cannot be created for " + object.getClass());
    }

    private BlobContainer globalMetadataContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID).add(GLOBAL_METADATA_PATH_TOKEN));
    }

    boolean isGlobalMetadataEqual(ClusterMetadataManifest first, ClusterMetadataManifest second, String clusterName) {
        Metadata secondGlobalMetadata = getGlobalMetadata(clusterName, second.getClusterUUID(), second);
        Metadata firstGlobalMetadata = getGlobalMetadata(clusterName, first.getClusterUUID(), first);
        return Metadata.isGlobalResourcesMetadataEquals(firstGlobalMetadata, secondGlobalMetadata);
    }

    private void setGlobalMetadataUploadTimeout(TimeValue newGlobalMetadataUploadTimeout) {
        this.globalMetadataUploadTimeout = newGlobalMetadataUploadTimeout;
    }

    public TimeValue getGlobalMetadataUploadTimeout() {
        return this.globalMetadataUploadTimeout;
    }
}
