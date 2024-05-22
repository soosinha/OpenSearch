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
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.common.io.Streams;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteCustomMetadata extends AbstractRemoteBlobStoreObject<Custom>{

    public static final String CUSTOM_METADATA = "custom";
    public static final String CUSTOM_DELIMITER = "--";

    public final ChecksumBlobStoreFormat<Custom> customBlobStoreFormat;

    private Custom custom;
    private String customType;
    private long metadataVersion;
    private final RemoteObjectStore<Custom> backingStore;
    private String blobName;
    private final NamedXContentRegistry xContentRegistry;
    private final String clusterUUID;

    public RemoteCustomMetadata(Custom custom, String customType, long metadataVersion,  String clusterUUID, RemoteObjectBlobStore<Custom> backingStore,
        NamedXContentRegistry xContentRegistry) {
        this.custom = custom;
        this.customType = customType;
        this.metadataVersion = metadataVersion;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
        this.customBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            "custom",
            METADATA_NAME_FORMAT,
            (parser -> Metadata.Custom.fromXContent(parser, customType))
        );
    }

    public RemoteCustomMetadata(String blobName, String customType, String clusterUUID, RemoteObjectStore<Custom> backingStore, NamedXContentRegistry xContentRegistry) {
        this.blobName = blobName;
        this.customType = customType;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
        this.customBlobStoreFormat = new ChecksumBlobStoreFormat<>(
            "custom",
            METADATA_NAME_FORMAT,
            (parser -> Metadata.Custom.fromXContent(parser, customType))
        );
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        String prefix = String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, customType);
        return new BlobPathParameters(List.of("global-metadata"), prefix);
    }

    @Override
    public String getFullBlobName() {
        return blobName;
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/<componentPrefix>__<inverted_metadata_version>__<inverted__timestamp>__<codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
        assert backingStore instanceof RemoteObjectBlobStore;
        RemoteObjectBlobStore<Custom> blobStore = (RemoteObjectBlobStore<Custom>) backingStore;
        // setting the full blob path with name for future access
        this.blobName = blobStore.getBlobPathForUpload(this).buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public Custom get() {
        return custom;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public RemoteObjectStore<Custom> getBackingStore() {
        return backingStore;
    }

    @Override
    public InputStream serialize() throws IOException {
        return customBlobStoreFormat.serialize(custom, generateBlobFileName(), getBackingStore().getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public Custom deserialize(InputStream inputStream) throws IOException {
        return customBlobStoreFormat.deserialize(blobName, xContentRegistry, Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(String.join(CUSTOM_DELIMITER, CUSTOM_METADATA, customType), blobName);
    }
}
