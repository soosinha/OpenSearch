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
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_CURRENT_CODEC_VERSION;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteCoordinationMetadata extends AbstractRemoteBlobStoreObject<CoordinationMetadata> {

    public static final String COORDINATION_METADATA = "coordination";
    public static final ChecksumBlobStoreFormat<CoordinationMetadata> COORDINATION_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "coordination",
        METADATA_NAME_FORMAT,
        CoordinationMetadata::fromXContent
    );

    private CoordinationMetadata coordinationMetadata;
    private long metadataVersion;
    private final RemoteObjectStore<CoordinationMetadata> backingStore;
    private String blobName;
    private final NamedXContentRegistry xContentRegistry;
    private final String clusterUUID;

    public RemoteCoordinationMetadata(CoordinationMetadata coordinationMetadata, long metadataVersion,  String clusterUUID, RemoteObjectBlobStore<CoordinationMetadata> backingStore,
        NamedXContentRegistry xContentRegistry) {
        this.coordinationMetadata = coordinationMetadata;
        this.metadataVersion = metadataVersion;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    public RemoteCoordinationMetadata(String blobName, String clusterUUID, RemoteObjectStore<CoordinationMetadata> backingStore, NamedXContentRegistry xContentRegistry) {
        this.blobName = blobName;
        this.backingStore = backingStore;
        this.xContentRegistry = xContentRegistry;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), COORDINATION_METADATA);
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
        RemoteObjectBlobStore<CoordinationMetadata> blobStore = (RemoteObjectBlobStore<CoordinationMetadata>) backingStore;
        // setting the full blob path with name for future access
        this.blobName = blobStore.getBlobPathForUpload(this).buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public CoordinationMetadata get() {
        return coordinationMetadata;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public RemoteObjectStore<CoordinationMetadata> getBackingStore() {
        return backingStore;
    }

    @Override
    public InputStream serialize() throws IOException {
        return COORDINATION_METADATA_FORMAT.serialize(coordinationMetadata, generateBlobFileName(), getBackingStore().getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public CoordinationMetadata deserialize(InputStream inputStream) throws IOException {
        return COORDINATION_METADATA_FORMAT.deserialize(blobName, xContentRegistry, Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(COORDINATION_METADATA, blobName);
    }
}
