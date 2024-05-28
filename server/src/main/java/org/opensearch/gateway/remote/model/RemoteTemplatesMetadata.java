/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_CURRENT_CODEC_VERSION;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.cluster.metadata.TemplatesMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

/**
 * Wrapper class for uploading/downloading {@link TemplatesMetadata} to/from remote blob store
 */
public class RemoteTemplatesMetadata extends AbstractRemoteBlobObject<TemplatesMetadata> {

    public static final String TEMPLATES_METADATA = "templates";

    public static final ChecksumBlobStoreFormat<TemplatesMetadata> TEMPLATES_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "templates",
        METADATA_NAME_FORMAT,
        TemplatesMetadata::fromXContent
    );
    private TemplatesMetadata templatesMetadata;
    private long metadataVersion;

    public RemoteTemplatesMetadata(TemplatesMetadata templatesMetadata, long metadataVersion, String clusterUUID, BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.templatesMetadata = templatesMetadata;
        this.metadataVersion = metadataVersion;
    }

    public RemoteTemplatesMetadata(String blobName, String clusterUUID, BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("global-metadata"), TEMPLATES_METADATA);
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/global-metadata/<componentPrefix>__<inverted_metadata_version>__<inverted__timestamp>__
        // <codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public TemplatesMetadata get() {
        return templatesMetadata;
    }

    @Override
    public InputStream serialize() throws IOException {
        return TEMPLATES_METADATA_FORMAT.serialize(templatesMetadata, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS)
            .streamInput();
    }

    @Override
    public TemplatesMetadata deserialize(InputStream inputStream) throws IOException {
        return TEMPLATES_METADATA_FORMAT.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(TEMPLATES_METADATA, blobName);
    }
}
