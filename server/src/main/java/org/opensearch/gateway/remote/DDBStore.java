/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import java.util.Map;

public class DDBStore implements Store<Map<String, String>, Map<String, String>> {

    @Override
    public void write(Map<String, String> key, Map<String, String> value) {

    }

    @Override
    public Map<String, String> read(Map<String, String> key) {
        return null;
    }
}
