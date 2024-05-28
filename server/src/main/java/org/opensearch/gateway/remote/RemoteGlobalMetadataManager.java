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
import static org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;

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
import org.opensearch.gateway.remote.model.AbstractRemoteBlobObject;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadata;
import org.opensearch.gateway.remote.model.RemoteCoordinationMetadataBlobStore;
import org.opensearch.gateway.remote.model.RemoteCustomMetadata;
import org.opensearch.gateway.remote.model.RemoteCustomMetadataBlobStore;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsBlobStore;
import org.opensearch.gateway.remote.model.RemotePersistentSettingsMetadata;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadata;
import org.opensearch.gateway.remote.model.RemoteTemplatesMetadataBlobStore;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsBlobStore;
import org.opensearch.gateway.remote.model.RemoteTransientSettingsMetadata;
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
    private final RemoteCoordinationMetadataBlobStore coordinationMetadataBlobStore;
    private final RemoteTransientSettingsBlobStore transientSettingsBlobStore;
    private final RemotePersistentSettingsBlobStore persistentSettingsBlobStore;
    private final RemoteTemplatesMetadataBlobStore templatesMetadataBlobStore;
    private final RemoteCustomMetadataBlobStore customMetadataBlobStore;

    RemoteGlobalMetadataManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, ThreadPool threadPool, BlobStoreTransferService blobStoreTransferService,
        String clusterName) {
        this.blobStoreRepository = blobStoreRepository;
        this.globalMetadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.threadPool = threadPool;
        this.blobStoreTransferService = blobStoreTransferService;
        this.clusterName = clusterName;
        this.coordinationMetadataBlobStore = new RemoteCoordinationMetadataBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.transientSettingsBlobStore = new RemoteTransientSettingsBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.persistentSettingsBlobStore = new RemotePersistentSettingsBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.templatesMetadataBlobStore = new RemoteTemplatesMetadataBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.customMetadataBlobStore = new RemoteCustomMetadataBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
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
        if (objectToUpload instanceof CoordinationMetadata) {
            RemoteCoordinationMetadata remoteCoordinationMetadata = new RemoteCoordinationMetadata((CoordinationMetadata) objectToUpload, metadataVersion, clusterUUID,
                blobStoreRepository);
            return () -> coordinationMetadataBlobStore.writeAsync(remoteCoordinationMetadata, getActionListener(remoteCoordinationMetadata, latchedActionListener));
        } else if (objectToUpload instanceof  Settings) {
            if (customType != null && customType.equals(TRANSIENT_SETTING_METADATA)) {
                RemoteTransientSettingsMetadata remoteTransientSettingsMetadata = new RemoteTransientSettingsMetadata((Settings) objectToUpload, metadataVersion, clusterUUID,
                    blobStoreRepository);
                return () -> transientSettingsBlobStore.writeAsync(remoteTransientSettingsMetadata, getActionListener(remoteTransientSettingsMetadata, latchedActionListener));
            }
            RemotePersistentSettingsMetadata remotePersistentSettingsMetadata = new RemotePersistentSettingsMetadata((Settings) objectToUpload, metadataVersion, clusterUUID,
                blobStoreRepository);
            return () -> persistentSettingsBlobStore.writeAsync(remotePersistentSettingsMetadata, getActionListener(remotePersistentSettingsMetadata, latchedActionListener));
        } else if (objectToUpload instanceof TemplatesMetadata) {
            RemoteTemplatesMetadata remoteTemplatesMetadata = new RemoteTemplatesMetadata((TemplatesMetadata) objectToUpload, metadataVersion, clusterUUID,
                blobStoreRepository);
            return () -> templatesMetadataBlobStore.writeAsync(remoteTemplatesMetadata, getActionListener(remoteTemplatesMetadata, latchedActionListener));
        } else if (objectToUpload instanceof Custom) {
            RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata((Custom) objectToUpload, customType ,metadataVersion, clusterUUID,
                blobStoreRepository);
            return () -> customMetadataBlobStore.writeAsync(remoteCustomMetadata, getActionListener(remoteCustomMetadata, latchedActionListener));
        }
        throw new RemoteStateTransferException("Remote object cannot be created for " + objectToUpload.getClass());
    }

    private ActionListener<Void> getActionListener(AbstractRemoteBlobObject remoteBlobStoreObject, LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        return ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteBlobStoreObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException("Upload failed", ex))
        );
    }

    CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterUUID,
        String component,
        String componentName,
        String uploadFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        ActionListener actionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, component, componentName)), listener::onFailure);
        if (component.equals(COORDINATION_METADATA)) {
            RemoteCoordinationMetadata remoteBlobStoreObject = new RemoteCoordinationMetadata(uploadFilename, clusterUUID, blobStoreRepository);
            return () -> coordinationMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(TEMPLATES_METADATA)) {
            RemoteTemplatesMetadata remoteBlobStoreObject = new RemoteTemplatesMetadata(uploadFilename, clusterUUID, blobStoreRepository);
            return () -> templatesMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(SETTING_METADATA)) {
            RemotePersistentSettingsMetadata remoteBlobStoreObject = new RemotePersistentSettingsMetadata(uploadFilename, clusterUUID, blobStoreRepository);
            return () -> persistentSettingsBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(TRANSIENT_SETTING_METADATA)) {
            RemoteTransientSettingsMetadata remoteBlobStoreObject = new RemoteTransientSettingsMetadata(uploadFilename, clusterUUID, blobStoreRepository);
            return () -> transientSettingsBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else if (component.equals(CUSTOM_METADATA)) {
            RemoteCustomMetadata remoteBlobStoreObject = new RemoteCustomMetadata(uploadFilename, componentName, clusterUUID, blobStoreRepository);
            return () -> customMetadataBlobStore.readAsync(remoteBlobStoreObject, actionListener);
        } else {
            throw new RemoteStateTransferException("Unknown component " + componentName);
        }
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
                builder.version(clusterMetadataManifest.getMetadataVersion());
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
                    blobStoreRepository);
                return coordinationMetadataBlobStore.read(remoteCoordinationMetadata);
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
                    blobStoreRepository);
                return persistentSettingsBlobStore.read(remotePersistentSettingsMetadata);
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
                    blobStoreRepository);
                return templatesMetadataBlobStore.read(remoteTemplatesMetadata);
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
            RemoteCustomMetadata remoteCustomMetadata = new RemoteCustomMetadata(customMetadataFileName, custom, clusterUUID, blobStoreRepository);
            return customMetadataBlobStore.read(remoteCustomMetadata);
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
