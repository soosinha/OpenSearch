/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.io.IOException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;

public interface RemoteObjectStore<T> {

    public void write(RemoteObject<T> remoteObject) throws IOException;
    public void writeAsync(RemoteObject<T> remoteObject, ActionListener<Void> listener);
    public T read(RemoteObject<T> remoteObject);

}
