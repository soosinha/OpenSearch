/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
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

    @Override
    public void write(RemoteObject<T> remoteObject) throws IOException {
        AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
        BytesReference bytesReference = abstractRemoteBlobStoreObject.serialize();
        abstractRemoteBlobStoreObject.getBlobContainer().writeBlob(abstractRemoteBlobStoreObject.generateBlobFileName(), bytesReference.streamInput(), bytesReference.length(), false);
    }

    @Override
    public CheckedRunnable<IOException> writeAsync(RemoteObject<T> remoteObject, ActionListener<Void> listener) {
        return () -> {
            AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
            BytesReference bytesReference = abstractRemoteBlobStoreObject.serialize();
            abstractRemoteBlobStoreObject.getBlobContainer().writeBlob(abstractRemoteBlobStoreObject.generateBlobFileName(), bytesReference.streamInput(), bytesReference.length(), false);
        };
    }

    @Override
    public T read(RemoteObject<T> remoteObject) throws IOException {
        AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
        return abstractRemoteBlobStoreObject.deserialize(getBlobContainer(abstractRemoteBlobStoreObject.getBlobPathParameters().getPathTokens()).readBlob(abstractRemoteBlobStoreObject.getBlobName()));
    }

    @Override
    public CompletableFuture<T> readAsync(RemoteObject<T> remoteObject){
        return CompletableFuture.supplyAsync(() -> {
            AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
            try {
                return abstractRemoteBlobStoreObject.deserialize(getBlobContainer(abstractRemoteBlobStoreObject.getBlobPathParameters().getPathTokens()).readBlob(abstractRemoteBlobStoreObject.getBlobName()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Compressor getCompressor() {
        return blobStoreRepository.getCompressor();
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
