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
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.compress.Compressor;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

public class RemoteObjectBlobStore<T> implements RemoteObjectStore<T> {

    private static final String PATH_DELIMITER = "/";

    private final ExecutorService executorService;
    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;

    public RemoteObjectBlobStore(ExecutorService executorService, BlobStoreTransferService blobStoreTransferService, BlobStoreRepository remoteStoreFactory, String clusterName) {
        this.executorService = executorService;
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = remoteStoreFactory;
        this.clusterName = clusterName;
    }

    @Override
    public CheckedRunnable<IOException> writeAsync(RemoteObject<T> remoteObject, ActionListener<Void> listener) {
        return () -> {
            AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
            InputStream inputStream = abstractRemoteBlobStoreObject.serialize();
            transferService.uploadBlob(inputStream, getBlobPathForUpload(remoteObject), abstractRemoteBlobStoreObject.getBlobFileName(), WritePriority.URGENT,
                listener);
        };
    }

    @Override
    public T read(RemoteObject<T> remoteObject) throws IOException {
        AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
        return remoteObject.deserialize(
            transferService.downloadBlob(getBlobPathForDownload(remoteObject), abstractRemoteBlobStoreObject.getBlobFileName()));
    }

    @Override
    public void readAsync(RemoteObject<T> remoteObject, ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(remoteObject));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public Compressor getCompressor() {
        return blobStoreRepository.getCompressor();
    }

    public BlobPath getBlobPathForUpload(RemoteObject<T> remoteObject) {
        BlobPath blobPath = blobStoreRepository.basePath().add(RemoteClusterStateUtils.encodeString(clusterName)).add("cluster-state").add(remoteObject.clusterUUID());
        AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
        for (String token : abstractRemoteBlobStoreObject.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    public BlobPath getBlobPathForDownload(RemoteObject<T> remoteObject) {
        AbstractRemoteBlobStoreObject<T> abstractRemoteBlobStoreObject = (AbstractRemoteBlobStoreObject<T>) remoteObject;
        String[] pathTokens = extractBlobPathTokens(abstractRemoteBlobStoreObject.getFullBlobName());
        BlobPath blobPath = blobStoreRepository.basePath();
        for (String token : pathTokens) {
            blobPath = blobPath.add(token);
        }
        return blobPath;
    }

    private static String extractBlobFileName(String blobName) {
        // todo add handling for *.dat extension
        assert blobName != null;
        String[] blobNameTokens = blobName.split(PATH_DELIMITER);
        return blobNameTokens[blobNameTokens.length - 1];
    }

    private static String[] extractBlobPathTokens(String blobName) {
        String[] blobNameTokens = blobName.split(PATH_DELIMITER);
        return Arrays.copyOfRange(blobNameTokens, 0, blobNameTokens.length - 1);
    }


}
