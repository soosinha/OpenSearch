/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.util.List;

public class BlobPathParameters {

    private List<String> pathTokens;
    private String filePrefix;

    public BlobPathParameters(List<String> pathTokens, String filePrefix) {
        this.pathTokens = pathTokens;
        this.filePrefix = filePrefix;
    }

    public List<String> getPathTokens() {
        return pathTokens;
    }

    public String getFilePrefix() {
        return filePrefix;
    }
}
