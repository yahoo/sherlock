/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.RedisConnection;
import com.yahoo.sherlock.store.core.SyncCommands;
import com.yahoo.sherlock.utils.TimeUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Job scheduling using a priority queue on clusters.
 */
@Slf4j
public class LettuceJobScheduler
        extends AbstractLettuceAccessor
        implements JobScheduler {

    /**
     * Redis command {@code ZREMRANGEBYSCORE} lacks {@code LIMIT}.
     * Modified from https://github.com/antirez/redis/issues/180.
     * Scripts are executed atomically.
     */
    public static final String SCRIPT_ZREMRANGEBYSCORE =
            "local entry = redis.call('zrangebyscore', KEYS[1], 0, tonumber(ARGV[1]), 'WITHSCORES', 'LIMIT', 0, 1);\n" +
                    "local pending = redis.call('zrangebyscore', KEYS[2], 0, tonumber(ARGV[1]) - 5, 'WITHSCORES');\n" +
                    "for i = 1, #pending, 2 do\n" +
                    "\tredis.call('zrem', KEYS[2], pending[i]);\n" +
                    "\tredis.call('zadd', KEYS[1], pending[i + 1], pending[i]);\n" +
                    "end\n" +
                    "if #entry < 2 then\n" +
                    "\treturn {};\n" +
                    "end\n" +
                    "local jobId = entry[1];\n" +
                    "local time = entry[2];\n" +
                    "redis.call('zrem', KEYS[1], jobId);\n" +
                    "redis.call('zadd', KEYS[2], time, jobId);\n" +
                    "return {jobId, time};";

    private String queueName;
    private String pendingQueueName;
    private final JobMetadataAccessor jobAccessor;

    /**
     * Uses hash tags on the pending queue and queue name to
     * ensure that the script executes.
     *
     * @param params store params
     */
    public LettuceJobScheduler(StoreParams params) {
        super(params);
        this.queueName = params.get(DatabaseConstants.QUEUE_JOB_SCHEDULE);
        this.pendingQueueName = params.get(DatabaseConstants.QUEUE_JOB_SCHEDULE) + "Pending";
        jobAccessor = Store.getJobMetadataAccessor();
        queueName = String.format("{queue}.%s", queueName);
        pendingQueueName = String.format("{queue}.%s", pendingQueueName);
    }

    @Override
    public void pushQueue(long timestampMinutes, String jobId) throws IOException {
        log.info("Pushing job [{}] with time [{}]", jobId, TimeUtils
            .getTimeFromSeconds(timestampMinutes * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS));
        try (RedisConnection<String> conn = connect()) {
            conn.sync().zadd(queueName, (double) timestampMinutes, jobId);
        }
    }

    @Override
    public void pushQueue(List<Pair<Integer, String>> jobsAndTimes) throws IOException {
        log.info("Pushing [{}] jobs to the queue", jobsAndTimes.size());
        try (RedisConnection<String> conn = connect()) {
            SyncCommands<String> syncCmd = conn.sync();
            for (Pair<Integer, String> jobAndTime : jobsAndTimes) {
                syncCmd.zadd(queueName, (double) jobAndTime.getLeft(), jobAndTime.getRight());
            }
        }
    }

    @Override
    public void removeQueue(String jobId) throws IOException {
        log.info("Attempting to remove job [{}] from the queue", jobId);
        try (RedisConnection<String> conn = connect()) {
            conn.sync().zrem(queueName, jobId);
        }
    }

    @Override
    public void removeQueue(Collection<String> jobIds) throws IOException {
        log.info("Attempting to remove [{}] jobs from the queue", jobIds.size());
        try (RedisConnection<String> conn = connect()) {
            SyncCommands<String> syncCmd = conn.sync();
            for (String id : jobIds) {
                syncCmd.zrem(queueName, id);
            }
        }
    }

    @Override
    public void removeAllQueue() throws IOException {
        log.info("Removing all jobs from the queue");
        try (RedisConnection<String> conn = connect()) {
            conn.sync().del(queueName, pendingQueueName);
        }
    }

    @Override
    public List<JobMetadata> getAllQueue() throws IOException {
        log.info("Retrieving all jobs from the queue");
        try (RedisConnection<String> conn = connect()) {
            List<ScoredValue<String>> jobs = conn.sync().zrangeWithScores(queueName, 0, -1);
            Set<String> ids = new HashSet<>((int) (1.5 * jobs.size()));
            for (ScoredValue<String> job : jobs) {
                ids.add(job.getValue());
            }
            return jobAccessor.getJobMetadata(ids);
        }
    }

    @Override
    public int peekQueue(long timestampMinutes) throws IOException {
        log.info("Peeking queue for time [{}]", timestampMinutes);
        try (RedisConnection<String> conn = connect()) {
            return conn.sync().zcount(queueName, Range.create((double) 0, (double) timestampMinutes)).intValue();
        }
    }

    @Override
    public JobMetadata popQueue(long timestampMinutes) throws IOException {
        log.debug("Popping one job from the queue with time [{}]", timestampMinutes);
        try (RedisConnection<String> conn = connect()) {
            SyncCommands<String> syncCmd = conn.sync();
            String[] keys = {queueName, pendingQueueName};
            List<Object> result = syncCmd.eval(
                    SCRIPT_ZREMRANGEBYSCORE,
                    ScriptOutputType.MULTI,
                    keys, String.valueOf(timestampMinutes));
            if (result.isEmpty()) {
                return null;
            }
            String jobId = (String) result.get(0);
            try {
                log.info("Found job [{}] on queue for time [{}]", jobId, TimeUtils
                    .getTimeFromSeconds(timestampMinutes * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS));
                return jobAccessor.getJobMetadata(jobId);
            } catch (JobNotFoundException e) {
                syncCmd.zrem(pendingQueueName, jobId);
                return null;
            }
        }
    }

    @Override
    public void removePending(String jobId) throws IOException {
        log.info("Removing job [{}] from the pending queue", jobId);
        try (RedisConnection<String> conn = connect()) {
            conn.sync().zrem(pendingQueueName, jobId);
        }
    }

    @Override
    public void removePending(Collection<String> jobIds) throws IOException {
        log.info("Attempting to remove [{}] jobs from pending queue", jobIds.size());
        try (RedisConnection<String> conn = connect()) {
            SyncCommands<String> syncCmd = conn.sync();
            for (String id : jobIds) {
                syncCmd.zrem(pendingQueueName, id);
            }
        }
    }
}
