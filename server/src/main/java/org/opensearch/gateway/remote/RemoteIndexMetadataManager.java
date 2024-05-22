/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;

import java.io.IOException;
import java.util.Locale;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

public class RemoteIndexMetadataManager {

    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );
    public static final String INDEX_PATH_TOKEN = "index";

    private final BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool;

    private final NamedXContentRegistry namedXContentRegistry;
    private final RemoteObjectBlobStore<IndexMetadata> remoteIndexMetadataBlobStore;

    private volatile TimeValue indexMetadataUploadTimeout;

    public RemoteIndexMetadataManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, ThreadPool threadPool, String clusterName,
        NamedXContentRegistry namedXContentRegistry, BlobStoreTransferService blobStoreTransferService) {
        this.blobStoreRepository = blobStoreRepository;
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        this.threadPool = threadPool;
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
        this.remoteIndexMetadataBlobStore = new RemoteObjectBlobStore<>(threadPool.executor(ThreadPool.Names.GENERIC), blobStoreTransferService, blobStoreRepository, clusterName);
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param indexMetadata {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    CheckedRunnable<IOException> getIndexMetadataAsyncAction(IndexMetadata indexMetadata, String clusterUUID,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(indexMetadata, clusterUUID, remoteIndexMetadataBlobStore, namedXContentRegistry);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                remoteIndexMetadata.getUploadedMetadata()
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().toString(), ex))
        );
        return remoteIndexMetadataBlobStore.writeAsync(remoteIndexMetadata, completionListener);
    }

    CheckedRunnable<IOException> getAsyncIndexMetadataReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<RemoteClusterStateUtils.RemoteReadResult> latchedActionListener
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(uploadedFilename, clusterUUID, remoteIndexMetadataBlobStore, blobStoreRepository.getNamedXContentRegistry());
        ActionListener<IndexMetadata> actionListener = ActionListener.wrap(
            //todo change dummy
            response -> latchedActionListener.onResponse(new RemoteClusterStateUtils.RemoteReadResult(response, INDEX_PATH_TOKEN, "dummy")),
            latchedActionListener::onFailure);
        return () -> remoteIndexMetadataBlobStore.readAsync(remoteIndexMetadata, actionListener);
//        String[] splitPath = uploadedFilename.split("/");
//        return () -> INDEX_METADATA_FORMAT.readAsync(
//            indexMetadataContainer(clusterName, clusterUUID, splitPath[0]),
//            splitPath[splitPath.length - 1],
//            blobStoreRepository.getNamedXContentRegistry(),
//            threadPool.executor(ThreadPool.Names.GENERIC),
//            ActionListener.wrap(
//                response -> latchedActionListener.onResponse(new RemoteClusterStateUtils.RemoteReadResult(response, INDEX_PATH_TOKEN, "dummy")),
//                latchedActionListener::onFailure)
//        );
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param uploadedIndexMetadata {@link ClusterMetadataManifest.UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    IndexMetadata getIndexMetadata(
        ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata, String clusterUUID, int manifestCodecVersion
    ) {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(RemoteClusterStateUtils.getFormattedFileName(
            uploadedIndexMetadata.getUploadedFilename(), manifestCodecVersion), clusterUUID, remoteIndexMetadataBlobStore, namedXContentRegistry);
        try {
            return remoteIndexMetadataBlobStore.read(remoteIndexMetadata);
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return this.indexMetadataUploadTimeout;
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

}
