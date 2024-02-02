/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Locale;
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
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.gateway.CorruptStateException;
import org.opensearch.index.store.exception.ChecksumCombinationException;
import org.opensearch.transport.BytesTransportRequest;

/**
 * ChecksumWritableFormat
 * @param <T>
 */
public class ChecksumWritableFormat <T extends Writeable> {
    // Serialization parameters to specify correct context for metadata serialization
    // The format version
    public static final int VERSION = 1;

    protected static final int BUFFER_SIZE = 4096;

    protected final String codec;

    private final String blobNameFormat;
    private NamedWriteableRegistry namedWriteableRegistry;
    private DiscoveryNode localNode;

    private final CheckedBiFunction<StreamInput, DiscoveryNode, T, IOException> reader;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     */
    public ChecksumWritableFormat(String codec, String blobNameFormat, CheckedBiFunction<StreamInput, DiscoveryNode, T, IOException> reader) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.codec = codec;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(BlobContainer blobContainer, String name) throws IOException {
        String blobName = blobName(name);
        return deserialize(blobName, Streams.readFully(blobContainer.readBlob(blobName)));
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    public T deserialize(String blobName, BytesReference bytes) throws IOException {
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
                T deserializedObj = reader.apply(in, localNode);
                return deserializedObj;
//                } else {
//                    throw new CorruptStateException("Cluster state could not be deserialized");
//                }
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally by compressed.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compressor          whether to use compression
     */
    public void write(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor
    ) throws IOException {
        final String blobName = blobName(name);
        final BytesReference bytes = serialize(obj, blobName, compressor);
        blobContainer.writeBlob(blobName, bytes.streamInput(), bytes.length(), false);
    }

    /**
     * Internally calls {@link #writeAsyncWithPriority} with {@link WritePriority#NORMAL}
     */
    public void writeAsync(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        ActionListener<Void> listener
    ) throws IOException {
        // use NORMAL priority by default
        this.writeAsyncWithPriority(obj, blobContainer, name, compressor, WritePriority.NORMAL, listener);
    }

    public void writeAsyncWithUrgentPriority(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        ActionListener<Void> listener
    ) throws IOException {
        this.writeAsyncWithPriority(obj, blobContainer, name, compressor, WritePriority.URGENT, listener);
    }

    /**
     * Method to writes blob with resolving the blob name using {@link #blobName} method with specified
     * {@link WritePriority}. Leverages the multipart upload if supported by the blobContainer.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compressor          whether to use compression
     * @param priority            write priority to be used
     * @param listener            listener to listen to write result
     */
    private void writeAsyncWithPriority(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        final WritePriority priority,
        ActionListener<Void> listener
    ) throws IOException {
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            write(obj, blobContainer, name, compressor);
            listener.onResponse(null);
            return;
        }
        final String blobName = blobName(name);
        final BytesReference bytes = serialize(obj, blobName, compressor);
        final String resourceDescription = "ChecksumBlobStoreFormat.writeAsyncWithPriority(blob=\"" + blobName + "\")";
        try (IndexInput input = new ByteArrayIndexInput(resourceDescription, BytesReference.toBytes(bytes))) {
            long expectedChecksum;
            try {
                expectedChecksum = checksumOfChecksum(input.clone(), 8);
            } catch (Exception e) {
                throw new ChecksumCombinationException(
                    "Potentially corrupted file: Checksum combination failed while combining stored checksum "
                        + "and calculated checksum of stored checksum",
                    resourceDescription,
                    e
                );
            }

            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    blobName,
                    blobName,
                    bytes.length(),
                    true,
                    priority,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                    expectedChecksum,
                    ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported()
                )
            ) {
                ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), listener);
            }
        }
    }

    public BytesReference serialize(final T obj, final String blobName, final Compressor compressor)
        throws IOException {
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
                    obj.writeTo(stream);
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
