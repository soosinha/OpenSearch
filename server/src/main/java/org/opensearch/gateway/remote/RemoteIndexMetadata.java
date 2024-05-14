/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteIndexMetadata extends AbstractRemoteBlobStoreObject<IndexMetadata> {

    public static final String DELIMITER = "__";
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        RemoteClusterStateUtils.METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    public static final ToXContent.Params FORMAT_PARAMS;

    static {
        Map<String, String> params = new HashMap<>(1);
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    private IndexMetadata indexMetadata;
    private final RemoteObjectStore<IndexMetadata> backingStore;
    private String blobName;

    public RemoteIndexMetadata(IndexMetadata indexMetadata, RemoteObjectStore<IndexMetadata> backingStore) {
        this.indexMetadata = indexMetadata;
        this.backingStore = backingStore;
    }

    public RemoteIndexMetadata(String blobName, RemoteObjectStore<IndexMetadata> backingStore) {
        this.blobName = blobName;
        this.backingStore = backingStore;
    }

    @Override
    public IndexMetadata get() {
        return indexMetadata;
    }

    @Override
    public BlobContainer getBlobContainer() {
        return null;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("index"), "metadata");
    }

    @Override
    public ChecksumBlobStoreFormat<IndexMetadata> getChecksumBlobStoreFormat() {
        return INDEX_METADATA_FORMAT;
    }

    @Override
    public String getBlobNameFormat() {
        return null;
    }

    @Override
    public String getBlobName() {
        return blobName;
    }

    @Override
    public String generateBlobFileName() {
        return String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION) // Keep the codec version at last place only, during read we reads last
            // place to determine codec version.
        );
    }

    @Override
    public RemoteObjectStore<IndexMetadata> getBackingStore() {
        return backingStore;
    }

    @Override
    public BytesReference serialize() throws IOException {
        return INDEX_METADATA_FORMAT.serialize(indexMetadata, generateBlobFileName(), getBackingStore().getCompressor(), FORMAT_PARAMS);
    }

    @Override
    public IndexMetadata deserialize(InputStream inputStream) throws IOException {
        return null;
    }
}
