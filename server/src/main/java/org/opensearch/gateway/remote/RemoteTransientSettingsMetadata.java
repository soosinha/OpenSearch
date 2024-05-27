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
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

public class RemoteTransientSettingsMetadata extends AbstractRemoteBlobStoreObject<Settings> {

    public static final String TRANSIENT_SETTING_METADATA = "transient-settings";

    public static final ChecksumBlobStoreFormat<Settings> SETTINGS_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "transient-settings",
        METADATA_NAME_FORMAT,
        Settings::fromXContent
    );

    private Settings transientSettings;
    private long metadataVersion;
    private String blobName;
    private final String clusterUUID;

    public RemoteTransientSettingsMetadata(Settings transientSettings, long metadataVersion,  String clusterUUID, BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
                                            ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.transientSettings = transientSettings;
        this.metadataVersion = metadataVersion;
        this.clusterUUID = clusterUUID;
    }

    public RemoteTransientSettingsMetadata(String blobName, String clusterUUID, BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
                                            ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.blobName = blobName;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), TRANSIENT_SETTING_METADATA);
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
        // setting the full blob path with name for future access
        this.blobName = getBlobPathForUpload().buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public Settings get() {
        return transientSettings;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }


    @Override
    public InputStream serialize() throws IOException {
        return SETTINGS_METADATA_FORMAT.serialize(transientSettings, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public Settings deserialize(InputStream inputStream) throws IOException {
        return SETTINGS_METADATA_FORMAT.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(TRANSIENT_SETTING_METADATA, blobName);
    }
}
