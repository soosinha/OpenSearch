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
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteTemplatesMetadata extends AbstractRemoteBlobStoreObject<TemplatesMetadata> {

    public static final String TEMPLATES_METADATA = "templates";

    public static final ChecksumBlobStoreFormat<TemplatesMetadata> TEMPLATES_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "templates",
        METADATA_NAME_FORMAT,
        TemplatesMetadata::fromXContent
    );
    private TemplatesMetadata templatesMetadata;
    private long metadataVersion;
    private final RemoteObjectStore<TemplatesMetadata> backingStore;
    private String blobName;
    private final NamedXContentRegistry xContentRegistry;
    private final String clusterUUID;
    public RemoteTemplatesMetadata(TemplatesMetadata templatesMetadata, long metadataVersion,  String clusterUUID, RemoteObjectBlobStore<TemplatesMetadata> backingStore,
        NamedXContentRegistry xContentRegistry) {
        this.templatesMetadata = templatesMetadata;
        this.metadataVersion = metadataVersion;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    public RemoteTemplatesMetadata(String blobName, String clusterUUID, RemoteObjectStore<TemplatesMetadata> backingStore, NamedXContentRegistry xContentRegistry) {
        this.blobName = blobName;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), TEMPLATES_METADATA);
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
        RemoteObjectBlobStore<TemplatesMetadata> blobStore = (RemoteObjectBlobStore<TemplatesMetadata>) backingStore;
        // setting the full blob path with name for future access
        this.blobName = blobStore.getBlobPathForUpload(this).buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public TemplatesMetadata get() {
        return templatesMetadata;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public RemoteObjectStore<TemplatesMetadata> getBackingStore() {
        return backingStore;
    }

    @Override
    public InputStream serialize() throws IOException {
        return TEMPLATES_METADATA_FORMAT.serialize(templatesMetadata, generateBlobFileName(), getBackingStore().getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public TemplatesMetadata deserialize(InputStream inputStream) throws IOException {
        return TEMPLATES_METADATA_FORMAT.deserialize(blobName, xContentRegistry, Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(TEMPLATES_METADATA, blobName);
    }
}
