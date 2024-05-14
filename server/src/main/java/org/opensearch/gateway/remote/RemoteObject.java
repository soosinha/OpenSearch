/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public interface RemoteObject <T> {
    public T getObject();
    public void upload(T object);
    public CheckedRunnable<IOException> uploadAsync();
    public T download();
    public T downloadAsync();
    public BlobContainer getBlobContainer();
    public BlobPathParameters getBlobPathParameters();
    public String getBlobNameFormat();
    public String generateBlobFileName();
    public RemoteObjectStore<T> getBackingStore();
    public BytesReference serialize();

}
