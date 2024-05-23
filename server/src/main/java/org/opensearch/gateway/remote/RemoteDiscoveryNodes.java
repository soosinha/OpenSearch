/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.Streams;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

public class RemoteDiscoveryNodes extends AbstractRemoteBlobStoreObject<DiscoveryNodes> {

    public static final String DISCOVERY_NODES = "nodes";
    public static final ChecksumBlobStoreFormat<DiscoveryNodes> DISCOVERY_NODES_FORMAT = new ChecksumBlobStoreFormat<>(
        "nodes",
        METADATA_NAME_FORMAT,
        DiscoveryNodes::fromXContent
    );

    private DiscoveryNodes discoveryNodes;
    private long stateVersion;
    private String blobName;
    private final String clusterUUID;

    public RemoteDiscoveryNodes(DiscoveryNodes discoveryNodes, long stateVersion,  String clusterUUID, BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.discoveryNodes = discoveryNodes;
        this.stateVersion = stateVersion;
        this.clusterUUID = clusterUUID;
    }

    public RemoteDiscoveryNodes(String blobName, String clusterUUID, BlobStoreTransferService blobStoreTransferService, BlobStoreRepository blobStoreRepository, String clusterName,
        ThreadPool threadPool) {
        super(blobStoreTransferService, blobStoreRepository, clusterName, threadPool);
        this.blobName = blobName;
        this.clusterUUID = clusterUUID;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of("transient"), DISCOVERY_NODES);
    }

    @Override
    public String getFullBlobName() {
        return blobName;
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/transient/<componentPrefix>__<inverted_state_version>__<inverted__timestamp>__<codec_version>
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(stateVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
        // setting the full blob path with name for future access
        this.blobName = getBlobPathForUpload().buildAsString() + blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedMetadataAttribute(DISCOVERY_NODES, blobName);
    }

    @Override
    public DiscoveryNodes get() {
        return discoveryNodes;
    }

    @Override
    public String clusterUUID() {
        return clusterUUID;
    }

    @Override
    public InputStream serialize() throws IOException {
        return DISCOVERY_NODES_FORMAT.serialize(discoveryNodes, generateBlobFileName(), getCompressor(), RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public DiscoveryNodes deserialize(InputStream inputStream) throws IOException {
        return DISCOVERY_NODES_FORMAT.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
