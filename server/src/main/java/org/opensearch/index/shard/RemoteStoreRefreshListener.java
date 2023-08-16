/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.UploadListener;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY;

/**
 * RefreshListener implementation to upload newly created segment files to the remote store
 *
 * @opensearch.internal
 */
public final class RemoteStoreRefreshListener extends CloseableRetryableRefreshListener {

    private final Logger logger;

    /**
     * The initial retry interval at which the retry job gets scheduled after a failure.
     */
    private static final int REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS = 1_000;

    /**
     * In an exponential back off setup, the maximum retry interval after the retry interval increases exponentially.
     */
    private static final int REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS = 10_000;

    private static final int INVALID_PRIMARY_TERM = -1;

    /**
     * Exponential back off policy with max retry interval.
     */
    private static final BackoffPolicy EXPONENTIAL_BACKOFF_POLICY = BackoffPolicy.exponentialEqualJitterBackoff(
        REMOTE_REFRESH_RETRY_BASE_INTERVAL_MILLIS,
        REMOTE_REFRESH_RETRY_MAX_INTERVAL_MILLIS
    );

    public static final Set<String> EXCLUDE_FILES = Set.of("write.lock");
    // Visible for testing
    public static final int LAST_N_METADATA_FILES_TO_KEEP = 10;

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final RemoteSegmentTransferTracker segmentTracker;
    private final Map<String, String> localSegmentChecksumMap;
    private long primaryTerm;
    private volatile Iterator<TimeValue> backoffDelayIterator;
    private final SegmentReplicationCheckpointPublisher checkpointPublisher;
    private final UploadListener statsListener;

    public RemoteStoreRefreshListener(
        IndexShard indexShard,
        SegmentReplicationCheckpointPublisher checkpointPublisher,
        RemoteSegmentTransferTracker segmentTracker
    ) {
        super(indexShard.getThreadPool());
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory = indexShard.store().directory();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        localSegmentChecksumMap = new HashMap<>();
        RemoteSegmentMetadata remoteSegmentMetadata = null;
        if (indexShard.routingEntry().primary()) {
            try {
                remoteSegmentMetadata = this.remoteDirectory.init();
            } catch (IOException e) {
                logger.error("Exception while initialising RemoteSegmentStoreDirectory", e);
            }
        }
        // initializing primary term with the primary term of latest metadata in remote store.
        // if no metadata is present, this value will be initialized with -1.
        this.primaryTerm = remoteSegmentMetadata != null ? remoteSegmentMetadata.getPrimaryTerm() : INVALID_PRIMARY_TERM;
        this.segmentTracker = segmentTracker;
        resetBackOffDelayIterator();
        this.checkpointPublisher = checkpointPublisher;
        this.statsListener = new UploadListener() {
            @Override
            public void beforeUpload(String file) {
                // Start tracking the upload bytes started
                segmentTracker.addUploadBytesStarted(segmentTracker.getLatestLocalFileNameLengthMap().get(file));
            }

            @Override
            public void onSuccess(String file) {
                // Track upload success
                segmentTracker.addUploadBytesSucceeded(segmentTracker.getLatestLocalFileNameLengthMap().get(file));
                segmentTracker.addToLatestUploadedFiles(file);
            }

            @Override
            public void onFailure(String file) {
                // Track upload failure
                segmentTracker.addUploadBytesFailed(segmentTracker.getLatestLocalFileNameLengthMap().get(file));
            }
        };
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    protected void runAfterRefreshExactlyOnce(boolean didRefresh) {
        if (shouldSync(didRefresh)) {
            segmentTracker.updateLocalRefreshTimeAndSeqNo();
            try {
                if (this.primaryTerm != indexShard.getOperationPrimaryTerm()) {
                    this.primaryTerm = indexShard.getOperationPrimaryTerm();
                    this.remoteDirectory.init();
                }
                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    Collection<String> localSegmentsPostRefresh = segmentInfosGatedCloseable.get().files(true);
                    updateLocalSizeMapAndTracker(localSegmentsPostRefresh);
                }
            } catch (Throwable t) {
                logger.error("Exception in runAfterRefreshExactlyOnce() method", t);
            }
        }
    }

    /**
     * Upload new segment files created as part of the last refresh to the remote segment store.
     * This method also uploads remote_segments_metadata file which contains metadata of each segment file uploaded.
     *
     * @param didRefresh true if the refresh opened a new reference
     * @return true if the method runs successfully.
     */
    @Override
    protected boolean performAfterRefreshWithPermit(boolean didRefresh) {
        boolean successful;
        // The third condition exists for uploading the zero state segments where the refresh has not changed the reader reference, but it
        // is important to upload the zero state segments so that the restore does not break.
        if (shouldSync(didRefresh)) {
            successful = syncSegments();
        } else {
            successful = true;
        }
        return successful;
    }

    private boolean shouldSync(boolean didRefresh) {
        return this.primaryTerm != indexShard.getOperationPrimaryTerm()
            || didRefresh
            || remoteDirectory.getSegmentsUploadedToRemoteStore().isEmpty();
    }

    private boolean syncSegments() {
        if (indexShard.getReplicationTracker().isPrimaryMode() == false || indexShard.state() == IndexShardState.CLOSED) {
            logger.info(
                "Skipped syncing segments with primaryMode={} indexShardState={}",
                indexShard.getReplicationTracker().isPrimaryMode(),
                indexShard.state()
            );
            return true;
        }
        ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();
        beforeSegmentsSync();
        long refreshTimeMs = segmentTracker.getLocalRefreshTimeMs(), refreshClockTimeMs = segmentTracker.getLocalRefreshClockTimeMs();
        long refreshSeqNo = segmentTracker.getLocalRefreshSeqNo();
        long bytesBeforeUpload = segmentTracker.getUploadBytesSucceeded(), startTimeInNS = System.nanoTime();
        final AtomicBoolean successful = new AtomicBoolean(false);

        try {
            try {
                // if a new segments_N file is present in local that is not uploaded to remote store yet, it
                // is considered as a first refresh post commit. A cleanup of stale commit files is triggered.
                // This is done to avoid delete post each refresh.
                if (isRefreshAfterCommit()) {
                    remoteDirectory.deleteStaleSegmentsAsync(LAST_N_METADATA_FILES_TO_KEEP);
                }

                try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = indexShard.getSegmentInfosSnapshot()) {
                    SegmentInfos segmentInfos = segmentInfosGatedCloseable.get();
                    assert segmentInfos.getGeneration() == checkpoint.getSegmentsGen() : "SegmentInfos generation: "
                        + segmentInfos.getGeneration()
                        + " does not match metadata generation: "
                        + checkpoint.getSegmentsGen();
                    // Capture replication checkpoint before uploading the segments as upload can take some time and checkpoint can
                    // move.
                    long lastRefreshedCheckpoint = ((InternalEngine) indexShard.getEngine()).lastRefreshedCheckpoint();
                    Collection<String> localSegmentsPostRefresh = segmentInfos.files(true);

                    // Create a map of file name to size and update the refresh segment tracker
                    updateLocalSizeMapAndTracker(localSegmentsPostRefresh);
                    CountDownLatch latch = new CountDownLatch(1);
                    ActionListener<Void> segmentUploadsCompletedListener = new LatchedActionListener<>(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            try {
                                // Start metadata file upload
                                uploadMetadata(localSegmentsPostRefresh, segmentInfos, checkpoint);
                                clearStaleFilesFromLocalSegmentChecksumMap(localSegmentsPostRefresh);
                                onSuccessfulSegmentsSync(
                                    refreshTimeMs,
                                    refreshClockTimeMs,
                                    refreshSeqNo,
                                    lastRefreshedCheckpoint,
                                    checkpoint
                                );
                                // At this point since we have uploaded new segments, segment infos and segment metadata file,
                                // along with marking minSeqNoToKeep, upload has succeeded completely.
                                successful.set(true);
                            } catch (Exception e) {
                                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                                // as part of exponential back-off retry logic. This should not affect durability of the indexed data
                                // with remote trans-log integration.
                                logger.warn("Exception in post new segment upload actions", e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Exception while uploading new segments to the remote segment store", e);
                        }
                    }, latch);

                    // Start the segments files upload
                    uploadNewSegments(localSegmentsPostRefresh, segmentUploadsCompletedListener);
                    latch.await();
                } catch (EngineException e) {
                    logger.warn("Exception while reading SegmentInfosSnapshot", e);
                }
            } catch (IOException e) {
                // We don't want to fail refresh if upload of new segments fails. The missed segments will be re-tried
                // as part of exponential back-off retry logic. This should not affect durability of the indexed data
                // with remote trans-log integration.
                logger.warn("Exception while uploading new segments to the remote segment store", e);
            }
        } catch (Throwable t) {
            logger.error("Exception in RemoteStoreRefreshListener.afterRefresh()", t);
        }
        updateFinalStatusInSegmentTracker(successful.get(), bytesBeforeUpload, startTimeInNS);
        // If there are failures in uploading segments, then we should retry as search idle can lead to
        // refresh not occurring until write happens.
        return successful.get();
    }

    /**
     * Clears the stale files from the latest local segment checksum map.
     *
     * @param localSegmentsPostRefresh list of segment files present post refresh
     */
    private void clearStaleFilesFromLocalSegmentChecksumMap(Collection<String> localSegmentsPostRefresh) {
        localSegmentChecksumMap.keySet()
            .stream()
            .filter(file -> !localSegmentsPostRefresh.contains(file))
            .collect(Collectors.toSet())
            .forEach(localSegmentChecksumMap::remove);
    }

    private void beforeSegmentsSync() {
        // Start tracking total uploads started
        segmentTracker.incrementTotalUploadsStarted();
    }

    private void onSuccessfulSegmentsSync(
        long refreshTimeMs,
        long refreshClockTimeMs,
        long refreshSeqNo,
        long lastRefreshedCheckpoint,
        ReplicationCheckpoint checkpoint
    ) {
        // Update latest uploaded segment files name in segment tracker
        segmentTracker.setLatestUploadedFiles(segmentTracker.getLatestLocalFileNameLengthMap().keySet());
        // Update the remote refresh time and refresh seq no
        updateRemoteRefreshTimeAndSeqNo(refreshTimeMs, refreshClockTimeMs, refreshSeqNo);
        // Reset the backoffDelayIterator for the future failures
        resetBackOffDelayIterator();
        // Set the minimum sequence number for keeping translog
        indexShard.getEngine().translogManager().setMinSeqNoToKeep(lastRefreshedCheckpoint + 1);
        // Publishing the new checkpoint which is used for remote store + segrep indexes
        checkpointPublisher.publish(indexShard, checkpoint);
    }

    /**
     * Resets the backoff delay iterator so that the next set of failures starts with the base delay and goes upto max delay.
     */
    private void resetBackOffDelayIterator() {
        backoffDelayIterator = EXPONENTIAL_BACKOFF_POLICY.iterator();
    }

    @Override
    protected TimeValue getNextRetryInterval() {
        return backoffDelayIterator.next();
    }

    @Override
    protected String getRetryThreadPoolName() {
        return ThreadPool.Names.REMOTE_REFRESH_RETRY;
    }

    private boolean isRefreshAfterCommit() throws IOException {
        String lastCommittedLocalSegmentFileName = SegmentInfos.getLastCommitSegmentsFileName(storeDirectory);
        return (lastCommittedLocalSegmentFileName != null
            && !remoteDirectory.containsFile(lastCommittedLocalSegmentFileName, getChecksumOfLocalFile(lastCommittedLocalSegmentFileName)));
    }

    void uploadMetadata(Collection<String> localSegmentsPostRefresh, SegmentInfos segmentInfos, ReplicationCheckpoint replicationCheckpoint)
        throws IOException {
        final long maxSeqNo = ((InternalEngine) indexShard.getEngine()).currentOngoingRefreshCheckpoint();
        SegmentInfos segmentInfosSnapshot = segmentInfos.clone();
        Map<String, String> userData = segmentInfosSnapshot.getUserData();
        userData.put(LOCAL_CHECKPOINT_KEY, String.valueOf(maxSeqNo));
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        segmentInfosSnapshot.setUserData(userData, false);

        Translog.TranslogGeneration translogGeneration = indexShard.getEngine().translogManager().getTranslogGeneration();
        if (translogGeneration == null) {
            throw new UnsupportedOperationException("Encountered null TranslogGeneration while uploading metadata to remote segment store");
        } else {
            long translogFileGeneration = translogGeneration.translogFileGeneration;
            remoteDirectory.uploadMetadata(
                localSegmentsPostRefresh,
                segmentInfosSnapshot,
                storeDirectory,
                translogFileGeneration,
                replicationCheckpoint
            );
        }
    }

    private void uploadNewSegments(Collection<String> localSegmentsPostRefresh, ActionListener<Void> listener) {
        Collection<String> filteredFiles = localSegmentsPostRefresh.stream().filter(file -> !skipUpload(file)).collect(Collectors.toList());
        if (filteredFiles.size() == 0) {
            listener.onResponse(null);
            return;
        }

        ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, filteredFiles.size());

        for (String src : filteredFiles) {
            ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                statsListener.onSuccess(src);
                batchUploadListener.onResponse(resp);
            }, ex -> {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
                statsListener.onFailure(src);
                batchUploadListener.onFailure(ex);
            });
            statsListener.beforeUpload(src);
            remoteDirectory.copyFrom(storeDirectory, src, IOContext.DEFAULT, aggregatedListener);
        }
    }

    /**
     * Whether to upload a file or not depending on whether file is in excluded list or has been already uploaded.
     *
     * @param file that needs to be uploaded.
     * @return true if the upload has to be skipped for the file.
     */
    private boolean skipUpload(String file) {
        try {
            // Exclude files that are already uploaded and the exclude files to come up with the list of files to be uploaded.
            return EXCLUDE_FILES.contains(file) || remoteDirectory.containsFile(file, getChecksumOfLocalFile(file));
        } catch (IOException e) {
            logger.error(
                "Exception while reading checksum of local segment file: {}, ignoring the exception and re-uploading the file",
                file
            );
        }
        return false;
    }

    private String getChecksumOfLocalFile(String file) throws IOException {
        if (!localSegmentChecksumMap.containsKey(file)) {
            try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.DEFAULT)) {
                String checksum = Long.toString(CodecUtil.retrieveChecksum(indexInput));
                localSegmentChecksumMap.put(file, checksum);
            }
        }
        return localSegmentChecksumMap.get(file);
    }

    /**
     * Updates the last refresh time and refresh seq no which is seen by remote store.
     */
    private void updateRemoteRefreshTimeAndSeqNo(long refreshTimeMs, long refreshClockTimeMs, long refreshSeqNo) {
        segmentTracker.updateRemoteRefreshClockTimeMs(refreshClockTimeMs);
        segmentTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        segmentTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
    }

    /**
     * Updates map of file name to size of the input segment files in the segment tracker. Uses {@code storeDirectory.fileLength(file)} to get the size.
     *
     * @param segmentFiles list of segment files that are part of the most recent local refresh.
     */
    private void updateLocalSizeMapAndTracker(Collection<String> segmentFiles) {
        segmentTracker.updateLatestLocalFileNameLengthMap(segmentFiles, storeDirectory::fileLength);
    }

    private void updateFinalStatusInSegmentTracker(boolean uploadStatus, long bytesBeforeUpload, long startTimeInNS) {
        if (uploadStatus) {
            long bytesUploaded = segmentTracker.getUploadBytesSucceeded() - bytesBeforeUpload;
            long timeTakenInMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeInNS);
            segmentTracker.incrementTotalUploadsSucceeded();
            segmentTracker.addUploadBytes(bytesUploaded);
            segmentTracker.addUploadBytesPerSec((bytesUploaded * 1_000L) / Math.max(1, timeTakenInMS));
            segmentTracker.addUploadTimeMs(timeTakenInMS);
        } else {
            segmentTracker.incrementTotalUploadsFailed();
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected boolean isRetryEnabled() {
        return true;
    }
}
