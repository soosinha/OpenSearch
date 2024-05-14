/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.util.Map;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

public class ChecksumBlobStoreFormatRegistry {

    public static Map<Class<?>, ChecksumBlobStoreFormat> checksumBlobStoreFormatMap;

}
