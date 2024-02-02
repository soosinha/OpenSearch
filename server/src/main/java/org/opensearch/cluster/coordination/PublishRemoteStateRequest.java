/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import java.io.IOException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * class to contain remote state publish details
 */
public class PublishRemoteStateRequest extends TermVersionRequest {

    private final String fromUuid;
    private final String toUuid;
    private final String manifestFilePath;
    private final String clusterStateFilePath;
    private final String diffFilePath;
    private final String clusterName;
    private final String clusterUuid;
    PublishRemoteStateRequest(DiscoveryNode sourceNode, long term, long version, String fromUuid, String toUuid, String manifestFilePath, String clusterStateFilePath, String diffFilePath, String clusterName, String clusterUuid) {
        super(sourceNode, term, version);
        this.fromUuid = fromUuid;
        this.toUuid = toUuid;
        this.manifestFilePath = manifestFilePath;
        this.clusterStateFilePath = clusterStateFilePath;
        this.diffFilePath = diffFilePath;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
    }

    PublishRemoteStateRequest(StreamInput in) throws IOException {
        super(in);
        this.fromUuid = in.readString();
        this.toUuid = in.readString();
        this.manifestFilePath = in.readString();
        this.clusterStateFilePath = in.readString();
        this.diffFilePath = in.readString();
        this.clusterName = in.readString();
        this.clusterUuid = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(fromUuid);
        out.writeString(toUuid);
        out.writeString(manifestFilePath);
        out.writeString(clusterStateFilePath);
        out.writeString(diffFilePath);
        out.writeString(clusterName);
        out.writeString(clusterUuid);
    }

    public String getFromUuid() {
        return fromUuid;
    }

    public String getToUuid() {
        return toUuid;
    }

    public String getManifestFilePath() {
        return manifestFilePath;
    }

    public String getClusterStateFilePath() {
        return clusterStateFilePath;
    }

    public String getDiffFilePath() {
        return diffFilePath;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    @Override
    public String toString() {
        return "PublishRemoteStateRequest{fromUuid=" + fromUuid + ", toUuid=" + toUuid + ", manifestFilePath=" + manifestFilePath + ", clusterStateFilePath=" + clusterStateFilePath + ", diffFilePath=" + diffFilePath + ", clusterName=" + clusterName + ", clusterUuid=" + clusterUuid + "}"; // to change
    }
}
