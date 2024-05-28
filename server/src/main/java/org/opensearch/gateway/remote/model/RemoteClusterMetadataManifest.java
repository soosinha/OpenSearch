/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.opensearch.common.io.Streams;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadataAttribute;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

/**
 * Wrapper class for uploading/downloading {@link ClusterMetadataManifest} to/from remote blob store
 */
public class RemoteClusterMetadataManifest extends AbstractRemoteBlobObject<ClusterMetadataManifest> {

    public static final String MANIFEST_PATH_TOKEN = "manifest";
    public static final int SPLITTED_MANIFEST_FILE_LENGTH = 6;

    public static final String MANIFEST_FILE_PREFIX = "manifest";
    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";
    public static final int MANIFEST_CURRENT_CODEC_VERSION = ClusterMetadataManifest.CODEC_V3;

    /**
     * Manifest format compatible with older codec v0, where codec version was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V0 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV0);
    /**
     * Manifest format compatible with older codec v1, where global metadata was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V1 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV1);

    /**
     * Manifest format compatible with codec v2, where we introduced codec versions/global metadata.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
    );

    private ClusterMetadataManifest clusterMetadataManifest;

    public RemoteClusterMetadataManifest(ClusterMetadataManifest clusterMetadataManifest, String clusterUUID,
        BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.clusterMetadataManifest = clusterMetadataManifest;
    }

    public RemoteClusterMetadataManifest(String blobName, String clusterUUID,
        BlobStoreRepository blobStoreRepository) {
        super(blobStoreRepository, clusterUUID);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(MANIFEST_PATH_TOKEN), MANIFEST_FILE_PREFIX);
    }

    @Override
    public String generateBlobFileName() {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest__<inverted_term>__<inverted_version>__C/P__<inverted__timestamp>__
        // <codec_version>
        String blobFileName = String.join(
            DELIMITER,
            MANIFEST_PATH_TOKEN,
            RemoteStoreUtils.invertLong(clusterMetadataManifest.getClusterTerm()),
            RemoteStoreUtils.invertLong(clusterMetadataManifest.getStateVersion()),
            (clusterMetadataManifest.isCommitted() ? "C" : "P"), // C for committed and P for published
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(clusterMetadataManifest.getCodecVersion()) // Keep the codec version at last place only, during read we reads last place to
            // determine codec version.
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        return new UploadedMetadataAttribute(MANIFEST_PATH_TOKEN, blobName);
    }

    @Override
    public ClusterMetadataManifest get() {
        return clusterMetadataManifest;
    }

    @Override
    public InputStream serialize() throws IOException {
        return CLUSTER_METADATA_MANIFEST_FORMAT.serialize(clusterMetadataManifest, generateBlobFileName(), getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS).streamInput();
    }

    @Override
    public ClusterMetadataManifest deserialize(InputStream inputStream) throws IOException {
        ChecksumBlobStoreFormat<ClusterMetadataManifest> blobStoreFormat = getClusterMetadataManifestBlobStoreFormat();
        return blobStoreFormat.deserialize(blobName, getBlobStoreRepository().getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

    private int getManifestCodecVersion() {
        assert blobName != null;
        String[] splitName = blobName.split(DELIMITER);
        if (splitName.length == SPLITTED_MANIFEST_FILE_LENGTH) {
            return Integer.parseInt(splitName[splitName.length - 1]); // Last value would be codec version.
        } else if (splitName.length < SPLITTED_MANIFEST_FILE_LENGTH) { // Where codec is not part of file name, i.e. default codec version 0
            // is used.
            return ClusterMetadataManifest.CODEC_V0;
        } else {
            throw new IllegalArgumentException("Manifest file name is corrupted");
        }
    }

    private ChecksumBlobStoreFormat<ClusterMetadataManifest> getClusterMetadataManifestBlobStoreFormat() {
        long codecVersion = getManifestCodecVersion();
        if (codecVersion == MANIFEST_CURRENT_CODEC_VERSION) {
            return CLUSTER_METADATA_MANIFEST_FORMAT;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V1) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V1;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V0) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V0;
        }
        throw new IllegalArgumentException("Cluster metadata manifest file is corrupted, don't have valid codec version");
    }
}
