/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.xcontent.ToXContent;

public class RemoteObjectManager<T extends ToXContent> {

    private final RemoteStoreFactory remoteStoreFactory;
    private final String clusterName;
    private final String clusterUUID;

    public RemoteObjectManager(RemoteStoreFactory remoteStoreFactory, String clusterName, String clusterUUID) {
        this.remoteStoreFactory = remoteStoreFactory;
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
    }

    public void write(RemoteObject<T> remoteObject) throws IOException {
        remoteObject.getBackingStore().write(remoteObject);
    }

    public T read(RemoteObject<T> remoteObject) throws IOException {
        return remoteObject.getBackingStore().read(remoteObject);
    }

}
