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
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.common.bytes.BytesReference;

public interface RemoteObject <T> {
    public T get();
    public String clusterUUID();
    public RemoteObjectStore<T> getBackingStore();
    public InputStream serialize() throws IOException;
    public T deserialize(InputStream inputStream) throws IOException;

}
