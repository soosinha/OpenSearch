/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CompressedStreamUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContent.Params;
import org.opensearch.gateway.CorruptStateException;
import org.opensearch.transport.BytesTransportRequest;

/**
 * ClusterStateBlobStoreFormat
 */
public class ClusterStateBlobStoreFormat extends ChecksumBlobStoreFormat {

    private NamedWriteableRegistry namedWriteableRegistry;
    private DiscoveryNode localNode;
    /**
     * @param codec codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     */
    public ClusterStateBlobStoreFormat(String codec, String blobNameFormat) {
        super(codec, blobNameFormat, null);
    }

    @Override
    public ClusterState deserialize(String blobName, NamedXContentRegistry namedXContentRegistry, BytesReference bytes) throws IOException {
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try {
            final IndexInput indexInput = bytes.length() > 0
                ? new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))), resourceDesc)
                : new ByteArrayIndexInput(resourceDesc, BytesRef.EMPTY_BYTES);
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);
            long filePointer = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;

            BytesTransportRequest bytesTransportRequest = new BytesTransportRequest(bytes.slice((int) filePointer, (int) contentSize), Version.CURRENT);

            try (StreamInput in = CompressedStreamUtils.decompressBytes(bytesTransportRequest, namedWriteableRegistry)) {
                ClusterState incomingState;
//                if (in.readBoolean()) {
                    // Close early to release resources used by the de-compression as early as possible
                    incomingState = ClusterState.readFrom(in, localNode);
                    return incomingState;
//                } else {
//                    throw new CorruptStateException("Cluster state could not be deserialized");
//                }
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    @Override
    public BytesReference serialize(ToXContent obj, String blobName, Compressor compressor, Params params) throws IOException {
        ClusterState clusterState = (ClusterState) obj;
        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")",
                    blobName,
                    outputStream,
                    BUFFER_SIZE
                )
            ) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                };
                    StreamOutput stream = new OutputStreamStreamOutput(CompressorRegistry.defaultCompressor().threadLocalOutputStream(indexOutputOutputStream));
                ) {
                    stream.setVersion(Version.CURRENT);
                    clusterState.writeTo(stream);
                }

                CodecUtil.writeFooter(indexOutput);
            }
            return outputStream.bytes();
        }
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    public void setNamedWriteableRegistry(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public DiscoveryNode getLocalNode() {
        return localNode;
    }

    public void setLocalNode(DiscoveryNode localNode) {
        this.localNode = localNode;
    }
}
