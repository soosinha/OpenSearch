/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.metadata.Metadata.Custom;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * An implementation of {@link RemoteObjectStore} which is used to upload/download {@link Custom} to/from blob store
 */
public class RemoteCustomMetadataBlobStore extends AbstractRemoteBlobStore<Custom, RemoteCustomMetadata> {

    public RemoteCustomMetadataBlobStore(BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository, String clusterName, ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
    }
}