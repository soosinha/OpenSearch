/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_CURRENT_CODEC_VERSION;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.common.io.Streams;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemotePersistentSettingsMetadata extends AbstractRemoteBlobStoreObject<Settings> {

    public static final String SETTING_METADATA = "settings";

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "settings",
        METADATA_NAME_FORMAT,
        Settings::fromXContent
    );

    private Settings persistentSettings;
    private long metadataVersion;
    private final RemoteObjectStore<Settings> backingStore;
    private String blobName;
    private final NamedXContentRegistry xContentRegistry;
    private final String clusterUUID;

    public RemotePersistentSettingsMetadata(Settings settings, long metadataVersion,  String clusterUUID, RemoteObjectBlobStore<Settings> backingStore,
        NamedXContentRegistry xContentRegistry) {
        this.persistentSettings = settings;
        this.metadataVersion = metadataVersion;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    public RemotePersistentSettingsMetadata(String blobName, String clusterUUID, RemoteObjectStore<Settings> backingStore, NamedXContentRegistry xContentRegistry) {
        this.blobName = blobName;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), SETTING_METADATA);
    }

    @Override
    public String getFullBlobName() {
        return blobName;
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
        assert backingStore instanceof RemoteObjectBlobStore;
        RemoteObjectBlobStore<Settings> blobStore = (RemoteObjectBlobStore<Settings>) backingStore;
        // setting the full blob path with name for future access
        this.blobName = blobStore.getBlobPathForUpload(this).buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public Settings get() {
        return persistentSettings;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public RemoteObjectStore<Settings> getBackingStore() {
        return backingStore;
    }

    @Override
    public InputStream serialize() throws IOException {
        return SETTINGS_METADATA_FORMAT.serialize(persistentSettings, generateBlobFileName(), getBackingStore().getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public Settings deserialize(InputStream inputStream) throws IOException {
        return SETTINGS_METADATA_FORMAT.deserialize(blobName, xContentRegistry, Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(SETTING_METADATA, blobName);
    }
}
