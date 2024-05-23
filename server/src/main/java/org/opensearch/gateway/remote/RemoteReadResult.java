/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.core.xcontent.ToXContent;

public class RemoteReadResult {
    ToXContent obj;
    String component;
    String componentName;

    public RemoteReadResult(ToXContent obj, String component, String componentName) {
        this.obj = obj;
        this.component = component;
        this.componentName = componentName;
    }

    public ToXContent getObj() {
        return obj;
    }

    public String getComponent() {
        return component;
    }

    public String getComponentName() {
        return componentName;
    }
}
