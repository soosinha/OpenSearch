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
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

public class RemoteClusterStateAttributesManager {
    public static final String CLUSTER_STATE_ATTRIBUTE = "cluster_state_attribute";
    public static final String DISCOVERY_NODES = "nodes";
    public static final String CLUSTER_BLOCKS = "blocks";
    public static final String CUSTOM_PREFIX = "custom";
    public static final int CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION = 1;
    private final BlobStoreTransferService blobStoreTransferService;
    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;
    private final String clusterName;

    RemoteClusterStateAttributesManager(BlobStoreTransferService blobStoreTransferService, BlobStoreRepository repository, ThreadPool threadPool, String clusterName) {
        this.blobStoreTransferService = blobStoreTransferService;
        this.blobStoreRepository = repository;
        this.threadPool = threadPool;
        this.clusterName = clusterName;
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
        AbstractRemoteBlobStoreObject remoteObject = getRemoteObject(componentData, clusterState.version(), clusterState.metadata().clusterUUID());
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteObject.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateUtils.RemoteStateTransferException(component, ex))
        );
        return remoteObject.writeAsync(completionListener);
    }

    private AbstractRemoteBlobStoreObject getRemoteObject(ToXContent componentData, long stateVersion, String clusterUUID) {
        if (componentData instanceof DiscoveryNodes) {
            return new RemoteDiscoveryNodes((DiscoveryNodes)componentData, stateVersion, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else if (componentData instanceof ClusterBlocks) {
            return new RemoteClusterBlocks((ClusterBlocks) componentData, stateVersion, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else {
            throw new RemoteStateTransferException("Remote object not found for "+ componentData.getClass());
        }
    }

    public CheckedRunnable<IOException> getAsyncMetadataReadAction(
        String clusterUUID,
        String component,
        String uploadedFilename,
        LatchedActionListener<RemoteReadResult> listener
    ) {
        AbstractRemoteBlobStoreObject remoteObject = getRemoteObject(component, uploadedFilename, clusterUUID);
        ActionListener actionListener = ActionListener.wrap(response -> listener.onResponse(new RemoteReadResult((ToXContent) response, CLUSTER_STATE_ATTRIBUTE, component)), listener::onFailure);
        return () -> remoteObject.readAsync(actionListener);
    }

    private AbstractRemoteBlobStoreObject getRemoteObject(String component, String blobName, String clusterUUID) {
        if (component.equals(RemoteDiscoveryNodes.DISCOVERY_NODES)) {
            return new RemoteDiscoveryNodes(blobName, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        } else if (component.equals(RemoteClusterBlocks.CLUSTER_BLOCKS)) {
            return new RemoteClusterBlocks(blobName, clusterUUID, blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        }  else {
            throw new RemoteStateTransferException("Remote object not found for "+ component);
        }
    }

}
