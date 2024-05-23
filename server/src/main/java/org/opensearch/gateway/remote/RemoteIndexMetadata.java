/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;


import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_PLAIN_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.Streams;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

public class RemoteIndexMetadata extends AbstractRemoteBlobStoreObject<IndexMetadata> {

    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_PLAIN_FORMAT,
        IndexMetadata::fromXContent
    );
    public static final String INDEX_PATH_TOKEN = "index";

    private IndexMetadata indexMetadata;
    private String blobName;
    private final String clusterUUID;

    public RemoteIndexMetadata(IndexMetadata indexMetadata, String clusterUUID,
        BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.indexMetadata = indexMetadata;
        this.clusterUUID = clusterUUID;
    }

    public RemoteIndexMetadata(String blobName, String clusterUUID,
        BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.blobName = blobName;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public IndexMetadata get() {
        return indexMetadata;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public String getFullBlobName() {
        return blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(INDEX_PATH_TOKEN), "metadata");
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            RemoteClusterStateUtils.DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION) // Keep the codec version at last place only, during reads we read last
            // place to determine codec version.
        );
        // setting the full blob path with name for future access
        this.blobName = getBlobPathForUpload().buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedIndexMetadata(indexMetadata.getIndexName(), indexMetadata.getIndexUUID(), blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return INDEX_METADATA_FORMAT.serialize(indexMetadata, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS)
            .streamInput();
    }

    @Override
    public IndexMetadata deserialize(InputStream inputStream) throws IOException {
        // Blob name parameter is redundant
        return INDEX_METADATA_FORMAT.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

}
