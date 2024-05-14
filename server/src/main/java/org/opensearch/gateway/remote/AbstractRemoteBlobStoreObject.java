/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public abstract class AbstractRemoteBlobStoreObject<T> implements RemoteObject <T> {

    public abstract BlobContainer getBlobContainer();
    public abstract BlobPathParameters getBlobPathParameters();
    public abstract String getBlobNameFormat();
    public abstract String getBlobName();
    public abstract String generateBlobFileName();
    public abstract RemoteObjectStore<T> getBackingStore();
    public abstract ChecksumBlobStoreFormat getChecksumBlobStoreFormat();
}
