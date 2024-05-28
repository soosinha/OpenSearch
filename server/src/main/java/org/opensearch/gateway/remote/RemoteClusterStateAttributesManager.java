/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import org.opensearch.gateway.remote.model.AbstractRemoteBlobObject;
import org.opensearch.gateway.remote.model.RemoteClusterBlocks;
import org.opensearch.gateway.remote.model.RemoteClusterBlocksBlobStore;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodes;
import org.opensearch.gateway.remote.model.RemoteDiscoveryNodesBlobStore;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

public class RemoteClusterStateAttributesManager {
    public static final String CLUSTER_STATE_ATTRIBUTE = "cluster_state_attribute";
    public static final String DISCOVERY_NODES = "nodes";
    public static final String CLUSTER_BLOCKS = "blocks";
    public static final int CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION = 1;
    private final BlobStoreTransferService blobStoreTransferService;
    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;
    private final String clusterName;
    private final RemoteClusterBlocksBlobStore clusterBlocksBlobStore;
    private final RemoteDiscoveryNodesBlobStore discoveryNodesBlobStore;

    RemoteClusterStateAttributesManager(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository repository, ThreadPool threadPool, String clusterName) {
        this.blobStoreTransferService = blobStoreTransferService;
        this.blobStoreRepository = repository;
        this.threadPool = threadPool;
        this.clusterName = clusterName;
        this.clusterBlocksBlobStore = new RemoteClusterBlocksBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.discoveryNodesBlobStore = new RemoteDiscoveryNodesBlobStore(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
    }

    /**
     * Allows async upload of Cluster State Attribute components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        ClusterState clusterState,
        String component,
        ToXContent componentData,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        if (componentData instanceof DiscoveryNodes) {
            RemoteDiscoveryNodes remoteObject = new RemoteDiscoveryNodes((DiscoveryNodes)componentData, clusterState.version(), clusterState.metadata().clusterUUID(), blobStoreRepository);
            return () -> discoveryNodesBlobStore.writeAsync(remoteObject, getActionListener(component, remoteObject, latchedActionListener));
        } else if (componentData instanceof ClusterBlocks) {
            RemoteClusterBlocks remoteObject = new RemoteClusterBlocks((ClusterBlocks) componentData, clusterState.version(), clusterState.metadata().clusterUUID(), blobStoreRepository);
            return () -> clusterBlocksBlobStore.writeAsync(remoteObject, getActionListener(component, remoteObject, latchedActionListener));
        } else {
            throw new RemoteStateTransferException("Remote object not found for "+ componentData.getClass());
        }
    }

    private ActionListener<Void> getActionListener(String component, AbstractRemoteBlobObject remoteObject, LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        return ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateUtils.RemoteStateTransferException(component, ex))
        );
    }

    public CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterUUID,
        String component,
        String uploadedFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        ActionListener actionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, CLUSTER_STATE_ATTRIBUTE, component)), listener::onFailure);
        if (component.equals(RemoteDiscoveryNodes.DISCOVERY_NODES)) {
            RemoteDiscoveryNodes remoteDiscoveryNodes = new RemoteDiscoveryNodes(uploadedFilename, clusterUUID, blobStoreRepository);
            return () -> discoveryNodesBlobStore.readAsync(remoteDiscoveryNodes, actionListener);
        } else if (component.equals(RemoteClusterBlocks.CLUSTER_BLOCKS)) {
            RemoteClusterBlocks remoteClusterBlocks = new RemoteClusterBlocks(uploadedFilename, clusterUUID, blobStoreRepository);
            return () -> clusterBlocksBlobStore.readAsync(remoteClusterBlocks, actionListener);
        }  else {
            throw new RemoteStateTransferException("Remote object not found for "+ component);
        }
    }
}
