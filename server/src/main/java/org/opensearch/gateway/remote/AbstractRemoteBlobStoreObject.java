/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.PATH_DELIMITER;

import org.opensearch.gateway.remote.ClusterMetadataManifest.UploadedMetadata;

public abstract class AbstractRemoteBlobStoreObject<T> implements RemoteObject <T> {

    public abstract BlobPathParameters getBlobPathParameters();
    public abstract String getFullBlobName();

    public String getBlobFileName() {
        if (getFullBlobName() == null) {
            generateBlobFileName();
        }
        String[] pathTokens = getFullBlobName().split(PATH_DELIMITER);
        return getFullBlobName().split(PATH_DELIMITER)[pathTokens.length - 1];
    }
    public abstract String generateBlobFileName();
    public abstract RemoteObjectStore<T> getBackingStore();
    public abstract UploadedMetadata getUploadedMetadata();

}
