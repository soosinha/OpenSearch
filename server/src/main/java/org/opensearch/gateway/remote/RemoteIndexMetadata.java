/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateService.METADATA_NAME_FORMAT;

import java.util.List;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class RemoteIndexMetadata extends AbstractRemoteObject<IndexMetadata> {

    public static final String DELIMITER = "__";
    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 1;

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );

    private final IndexMetadata indexMetadata;
    private final RemoteObjectStore<IndexMetadata> backingStore;

    public RemoteIndexMetadata(IndexMetadata indexMetadata, RemoteObjectStore<IndexMetadata> backingStore) {
        this.indexMetadata = indexMetadata;
        this.backingStore = backingStore;
    }

    @Override
    public IndexMetadata getObject() {
        return indexMetadata;
    }

    @Override
    public void upload(IndexMetadata indexMetadata) {
        getBackingStore().write(this);
    }

    @Override
    public void uploadAsync() {

    }

    @Override
    public IndexMetadata download() {
        return getBackingStore().read(this);
    }

    @Override
    public IndexMetadata downloadAsync() {

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

    public BytesReference serialize() {
        INDEX_METADATA_FORMAT.serialize(indexMetadata, generateBlobFileName(), backingStore)
    }
}
