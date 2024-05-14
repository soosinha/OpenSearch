/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;


import java.io.InputStream;

public abstract class BlobStore implements Store<java.lang.String, java.io.InputStream> {

    @Override
    public abstract void write(java.lang.String key, java.io.InputStream value);

    @Override
    public abstract java.io.InputStream read(java.lang.String key);
}
