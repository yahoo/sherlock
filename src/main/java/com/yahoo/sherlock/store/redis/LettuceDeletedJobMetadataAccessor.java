/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import io.lettuce.core.RedisFuture;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Deleted job metadata accessor for clusters.
 */
@Slf4j
public class LettuceDeletedJobMetadataAccessor
        extends AbstractLettuceAccessor
    implements DeletedJobMetadataAccessor {

    private final String deletedName;

    /**
     * @param params store params
     */
    public LettuceDeletedJobMetadataAccessor(StoreParams params) {
        super(params);
        this.deletedName = params.get(DatabaseConstants.INDEX_DELETED_ID);
    }

    /**
     * @param job job to check ID
     * @return whether the job has an ID
     */
    protected static boolean isMissingId(JobMetadata job) {
        return job.getJobId() == null;
    }

    @Override
    public void putDeletedJobMetadata(JobMetadata job) throws IOException {
        log.info("Putting deleted job metadata [{}]", job.getJobId());
        try (RedisConnection<String> conn = connect()) {
            if (isMissingId(job)) {
                job.setJobId(newId());
            }
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<String> hmset = cmd.hmset(key(job.getJobId()), map(job));
            RedisFuture<Long> sadd = cmd.sadd(index(deletedName, "all"), job.getJobId().toString());
            cmd.flushCommands();
            await(hmset, sadd);
        }
    }

    @Override
    public JobMetadata getDeletedJobMetadata(String jobId) throws IOException, JobNotFoundException {
        log.info("Getting deleted job metadata with  ID [{}]", jobId);
        try (RedisConnection<String> conn = connect()) {
            Map<String, String> jobMap = conn.sync().hgetall(key(jobId));
            if (jobMap.isEmpty()) {
                throw new JobNotFoundException(jobId);
            }
            return unmap(JobMetadata.class, jobMap);
        }
    }

    @Override
    public List<JobMetadata> getDeletedJobMetadataList() throws IOException {
        log.info("Getting list of deleted jobs");
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            Set<String> jobIds = conn.sync().smembers(index(deletedName, "all"));
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(jobIds.size());
            cmd.setAutoFlushCommands(false);
            for (String jobId : jobIds) {
                values.add(cmd.hgetall(key(jobId)));
            }
            cmd.flushCommands();
            await(values);
            List<JobMetadata> jobs = new ArrayList<>(values.size());
            for (RedisFuture<Map<String, String>> value : values) {
                jobs.add(unmap(JobMetadata.class, value.get()));
            }
            log.info("Successfully retrieved [{}] deleted jobs", jobs.size());
            return jobs;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while getting jobs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void putDeletedJobMetadata(Collection<JobMetadata> jobs) throws IOException {
        log.info("Putting [{}] deleted jobs", jobs.size());
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            List<JobMetadata> requireId = new ArrayList<>(jobs.size());
            List<JobMetadata> ready = new ArrayList<>(jobs.size());
            cmd.setAutoFlushCommands(false);
            for (JobMetadata job : jobs) {
                if (isMissingId(job)) {
                    requireId.add(job);
                } else {
                    ready.add(job);
                }
            }
            if (!requireId.isEmpty()) {
                Integer[] newIds = newIds(requireId.size());
                for (int i = 0; i < newIds.length; i++) {
                    requireId.get(i).setJobId(newIds[i]);
                }
                ready.addAll(requireId);
                requireId.clear();
            }
            RedisFuture[] futures = new RedisFuture[2 * ready.size()];
            int i = 0;
            for (JobMetadata job : ready) {
                futures[i++] = cmd.hmset(key(job.getJobId()), map(job));
                futures[i++] = cmd.sadd(index(deletedName, "all"), job.getJobId().toString());
            }
            cmd.flushCommands();
            await(futures);
        }
    }
}
