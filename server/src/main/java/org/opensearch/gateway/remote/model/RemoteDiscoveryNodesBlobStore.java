/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

/**
 * An implementation of {@link RemoteObjectStore} which is used to upload/download {@link DiscoveryNodes} to/from blob store
 */
public class RemoteDiscoveryNodesBlobStore extends AbstractRemoteBlobStore<DiscoveryNodes, RemoteDiscoveryNodes> {

    public RemoteDiscoveryNodesBlobStore(BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository, String clusterName, ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
    }
}
