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
import org.opensearch.core.action.ActionListener;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

public class RemoteObjectBlobStore<T> implements RemoteObjectStore<T> {
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final String clusterUUID;

    public RemoteObjectBlobStore(BlobStoreRepository remoteStoreFactory, String clusterName, String clusterUUID) {
        this.blobStoreRepository = remoteStoreFactory;
        this.clusterName = clusterName;
        this.clusterUUID = clusterUUID;
    }

    public void write(RemoteObject<T> remoteObject) throws IOException {
        // serialize using remoteObject.serialize()
        // abstract out the methods from ChecksumBlobStoreFormat to write to blob store
        // remoteObject.getChecksumBlobStoreFormat().write(remoteObject.getObject(), getBlobContainer(remoteObject.getBlobPathParameters().getPathTokens()), remoteObject.generateBlobFileName(), blobStoreRepository.getCompressor());
    }

    @Override
    public void writeAsync(RemoteObject<T> remoteObject, ActionListener<Void> listener) {
        remoteObject.getChecksumBlobStoreFormat().writeAsyncWithUrgentPriority(remoteObject.getObject(), );
    }

    public T read(RemoteObject<T> remoteObject) {
        return remoteObject.getChecksumBlobStoreFormat().read(getBlobContainer(remoteObject.getBlobPathParameters().getPathTokens()), remoteObject.getBlobNameFormat(), blobStoreRepository.getNamedXContentRegistry());
    }

    private BlobContainer getBlobContainer(List<String> pathTokens) {
        BlobPath blobPath = blobStoreRepository.basePath().add(encodeString(clusterName)).add("cluster-state").add(clusterUUID);
        for (String token : pathTokens) {
            blobPath = blobPath.add(token);
        }
        return blobStoreRepository.blobStore().blobContainer(blobPath);
    }

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

}
