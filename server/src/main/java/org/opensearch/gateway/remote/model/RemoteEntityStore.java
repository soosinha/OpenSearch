/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import java.io.IOException;
import org.opensearch.core.action.ActionListener;

/**
 * An interface to read/write an object from/to a remote storage. This interface is agnostic of the remote storage type.
 *
 * @param <T> The object type which can be uploaded to or downloaded from remote storage.
 */
public interface RemoteEntityStore<T extends RemoteEntity<T>> {

    public void writeAsync(T obj, ActionListener<Void> listener);

    public T read(T obj) throws IOException;

    public void readAsync(T obj, ActionListener<T> listener);
}
