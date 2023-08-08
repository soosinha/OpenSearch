package org.opensearch.cluster.store;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

public class ClusterMetadataMarker implements Writeable, ToXContentFragment {

    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField TERM_FIELD = new ParseField("term");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField CLUSTER_UUID_FIELD = new ParseField("cluster_uuid");
    private static final ParseField STATE_UUID_FIELD = new ParseField("state_uuid");

    private static Map<String, UploadedIndexMetadata> indices(Object[] fields) {
        return new HashMap<>((Map<String, UploadedIndexMetadata>) fields[0]);
    }

    private static long term(Object[] fields) {
        return (long) fields[1];
    }

    private static long version(Object[] fields) {
        return (long) fields[2];
    }

    private static String clusterUUID(Object[] fields) {
        return (String) fields[3];
    }

    private static String stateUUID(Object[] fields) {
        return (String) fields[4];
    }

    private static final ConstructingObjectParser<ClusterMetadataMarker, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_metadata_marker",
        fields -> new ClusterMetadataMarker(indices(fields), term(fields), version(fields), clusterUUID(fields), stateUUID(fields))
    );

    private final Map<String, UploadedIndexMetadata> indices;
    private final long term;
    private final long version;
    private final String clusterUUID;
    private final String stateUUID;

    public Map<String, UploadedIndexMetadata> getIndices() {
        return indices;
    }

    public long getTerm() {
        return term;
    }

    public long getVersion() {
        return version;
    }

    public String getClusterUUID() {
        return clusterUUID;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public ClusterMetadataMarker(Map<String, UploadedIndexMetadata> indices, long term, long version, String clusterUUID, String stateUUID) {
        this.indices = Collections.unmodifiableMap(indices);
        this.term = term;
        this.version = version;
        this.clusterUUID = clusterUUID;
        this.stateUUID = stateUUID;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(INDICES_FIELD.getPreferredName(), getIndices())
            .field(TERM_FIELD.getPreferredName(), getTerm())
            .field(VERSION_FIELD.getPreferredName(), getVersion())
            .field(CLUSTER_UUID_FIELD.getPreferredName(), getClusterUUID())
            .field(STATE_UUID_FIELD.getPreferredName(), getStateUUID());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMapWithConsistentOrder(indices);
        out.writeVLong(term);
        out.writeVLong(version);
        out.writeString(clusterUUID);
        out.writeString(stateUUID);
    }

    public static ClusterMetadataMarker fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class Builder {

        private final Map<String, UploadedIndexMetadata> indices;
        private long term;
        private long version;
        private String clusterUUID;
        private String stateUUID;


        public void term(long term) {
            this.term = term;
        }

        public void version(long version) {
            this.version = version;
        }

        public void clusterUUID(String clusterUUID) {
            this.clusterUUID = clusterUUID;
        }

        public void stateUUID(String stateUUID) {
            this.stateUUID = stateUUID;
        }

        public Map<String, UploadedIndexMetadata> getIndices() {
            return indices;
        }

        public Builder() {
            indices = new HashMap<>();
        }

        public ClusterMetadataMarker build() {
            return new ClusterMetadataMarker(indices, term, version, clusterUUID, stateUUID);
        }

    }

    public static class UploadedIndexMetadata implements Writeable, ToXContentFragment {

        private static final ParseField UPLOADED_FILENAME_FIELD = new ParseField("uploaded_filename");

        private static String uploadedFilename(Object[] fields) {
            return (String) fields[0];
        }

        private static final ConstructingObjectParser<UploadedIndexMetadata, Void> PARSER = new ConstructingObjectParser<>("uploaded_index_metadata",
            fields -> new UploadedIndexMetadata(uploadedFilename(fields)));

        private final String uploadedFilename;

        public UploadedIndexMetadata(String uploadedFileName) {
            this.uploadedFilename = uploadedFileName;
        }

        public String getUploadedFilename() {
            return uploadedFilename;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(UPLOADED_FILENAME_FIELD.getPreferredName(), getUploadedFilename());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(uploadedFilename);
        }

        public static UploadedIndexMetadata fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }
    }
}
