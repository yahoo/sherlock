package com.yahoo.sherlock.store.redis;

import com.google.common.collect.Lists;
import com.lambdaworks.redis.RedisFuture;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.exception.JobNotFoundException;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DeletedJobMetadataAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.store.StoreParams;
import com.yahoo.sherlock.store.core.AsyncCommands;
import com.yahoo.sherlock.store.core.RedisConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Job metadata accessor implemented for clusters with lettuce.
 */
@Slf4j
public class LettuceJobMetadataAccessor
        extends AbstractLettuceAccessor
        implements JobMetadataAccessor {

    private final String jobIdName;
    private final String jobStatusName;
    private final String clusterIdName;

    private final DeletedJobMetadataAccessor deletedAccessor;
    private final AnomalyReportAccessor anomalyReportAccessor;
    private final EmailMetadataAccessor emailMetadataAccessor;

    /**
     * @param params store parameters
     */
    public LettuceJobMetadataAccessor(StoreParams params) {
        super(params);
        this.jobIdName = params.get(DatabaseConstants.INDEX_JOB_ID);
        this.jobStatusName = params.get(DatabaseConstants.INDEX_JOB_STATUS);
        this.clusterIdName = params.get(DatabaseConstants.INDEX_JOB_CLUSTER_ID);
        deletedAccessor = Store.getDeletedJobMetadataAccessor();
        anomalyReportAccessor = Store.getAnomalyReportAccessor();
        emailMetadataAccessor = Store.getEmailMetadataAccessor();
    }

    /**
     * @param job job to check ID
     * @return whether the job has an assigned ID
     */
    protected static boolean isMissingId(JobMetadata job) {
        return job.getJobId() == null;
    }

    /**
     * Delete a job with the given ID.
     *
     * @param jobId job ID to delete
     * @return the deleted job
     * @throws IOException          if an error occurs with redis
     * @throws JobNotFoundException if the job does not exist
     */
    public JobMetadata performDeleteJob(String jobId) throws IOException, JobNotFoundException {
        log.info("Deleting job [{}] from the store", jobId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Long> delId = cmd.srem(index(jobIdName, "all"), jobId);
            RedisFuture<Map<String, String>> value = cmd.hgetall(key(jobId));
            cmd.flushCommands();
            await(delId, value);
            if (value.get().isEmpty()) {
                throw new JobNotFoundException(jobId);
            }
            JobMetadata job = unmap(JobMetadata.class, value.get());
            RedisFuture<Long> delStatus = cmd.srem(index(jobStatusName, job.getJobStatus()), jobId);
            RedisFuture<Long> delCluster = cmd.srem(index(clusterIdName, job.getClusterId()), jobId);
            RedisFuture<Long> delValue = cmd.del(key(jobId));
            cmd.flushCommands();
            await(delStatus, delCluster, delValue);
            anomalyReportAccessor.deleteAnomalyReportsForJob(jobId);
            log.info("Successfully deleted job [{}]", jobId);
            return job;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while deleting job!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Delete a set of jobs.
     *
     * @param jobIds job IDs to delete
     * @return the set of deleted jobs
     * @throws IOException if an error occurs
     */
    public Set<JobMetadata> performDeleteJob(Set<String> jobIds) throws IOException {
        log.info("Deleting [{}] jobs from the store", jobIds.size());
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(jobIds.size());
            List<RedisFuture<Long>> delIds = new ArrayList<>(jobIds.size());
            cmd.setAutoFlushCommands(false);
            for (String jobId : jobIds) {
                delIds.add(cmd.srem(index(jobIdName, "all"), jobId));
                values.add(cmd.hgetall(key(jobId)));
            }
            cmd.flushCommands();
            List<RedisFuture[]> futures = Lists.newArrayList(
                    values.toArray(new RedisFuture[values.size()]),
                    delIds.toArray(new RedisFuture[delIds.size()])
            );
            awaitCollection(futures);
            Set<JobMetadata> jobs = new HashSet<>((int) (1.5 * values.size()));
            RedisFuture[] futureArr = new RedisFuture[3 * values.size()];
            int i = 0;
            for (RedisFuture<Map<String, String>> value : values) {
                JobMetadata job = unmap(JobMetadata.class, value.get());
                jobs.add(job);
                futureArr[i++] = cmd.srem(index(jobStatusName, job.getJobStatus()), job.getJobId().toString());
                futureArr[i++] = cmd.srem(index(clusterIdName, job.getClusterId()), job.getJobId().toString());
                futureArr[i++] = cmd.del(key(job.getJobId()));
            }
            cmd.flushCommands();
            await(futureArr);
            for (String jobId : jobIds) {
                anomalyReportAccessor.deleteAnomalyReportsForJob(jobId);
            }
            log.info("Successfully delete [{}] jobs", jobs.size());
            return jobs;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while deleting jobs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Delete a set of jobs where the jobs are given.
     *
     * @param jobs jobs to delete
     */
    public void deleteGivenJobs(Set<JobMetadata> jobs) {
        log.info("Deleting [{}] given jobs", jobs.size());
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            RedisFuture[] futures = new RedisFuture[4 * jobs.size()];
            cmd.setAutoFlushCommands(false);
            int i = 0;
            for (JobMetadata job : jobs) {
                String id = job.getJobId().toString();
                futures[i++] = cmd.srem(index(jobIdName, "all"), id);
                futures[i++] = cmd.srem(index(jobStatusName, job.getJobStatus()), id);
                futures[i++] = cmd.srem(index(clusterIdName, job.getClusterId()), id);
                futures[i++] = cmd.del(key(id));
            }
            cmd.flushCommands();
            await(futures);
        }
    }

    @Override
    public List<JobMetadata> getJobMetadata(Set<String> jobIds) throws IOException {
        log.info("Getting list of [{}] jobs", jobIds.size());
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            List<RedisFuture<Map<String, String>>> values = new ArrayList<>(jobIds.size());
            cmd.setAutoFlushCommands(false);
            for (String id : jobIds) {
                values.add(cmd.hgetall(key(id)));
            }
            cmd.flushCommands();
            await(values);
            List<JobMetadata> jobs = new ArrayList<>(values.size());
            for (RedisFuture<Map<String, String>> value : values) {
                jobs.add(unmap(JobMetadata.class, value.get()));
            }
            return jobs;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error occurred while getting jobs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public JobMetadata getJobMetadata(String jobId) throws IOException, JobNotFoundException {
        log.info("Getting job metadata [{}]", jobId);
        try (RedisConnection<String> conn = connect()) {
            Map<String, String> jobMap = conn.sync().hgetall(key(jobId));
            if (jobMap.isEmpty()) {
                throw new JobNotFoundException(jobId);
            }
            return unmap(JobMetadata.class, jobMap);
        }
    }

    @Override
    public String putJobMetadata(JobMetadata job) throws IOException {
        log.info("Putting job metadata with ID [{}]", job.getJobId());
        try (RedisConnection<String> conn = connect()) {
            if (isMissingId(job)) {
                job.setJobId(newId());
            }
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            String jobId = job.getJobId().toString();
            RedisFuture[] futures = {
                    cmd.hmset(key(job.getJobId()), map(job)),
                    cmd.sadd(index(jobIdName, "all"), jobId),
                    cmd.sadd(index(jobStatusName, job.getJobStatus()), jobId),
                    cmd.sadd(index(clusterIdName, job.getClusterId()), jobId)
            };
            cmd.flushCommands();
            await(futures);
            String[] emails = job.getOwnerEmail() == null || job.getOwnerEmail().isEmpty() ? new String[0] : job.getOwnerEmail().split(Constants.COMMA_DELIMITER);
            if (emails.length != 0) {
                for (String email : emails) {
                    emailMetadataAccessor.putEmailMetadataIfNotExist(email, jobId);
                }
            }
            log.info("Job metadata with ID [{}] is updated", job.getJobId());
            return String.valueOf(job.getJobId());
        }
    }

    @Override
    public void putJobMetadata(List<JobMetadata> jobs) throws IOException {
        log.info("Putting list of [{}] jobs", jobs.size());
        try (RedisConnection<String> conn = connect()) {
            List<JobMetadata> requireId = new ArrayList<>(jobs.size());
            for (JobMetadata job : jobs) {
                if (isMissingId(job)) {
                    requireId.add(job);
                }
            }
            if (!requireId.isEmpty()) {
                Integer[] newIds = newIds(requireId.size());
                for (int i = 0; i < newIds.length; i++) {
                    requireId.get(i).setJobId(newIds[i]);
                }
            }
            AsyncCommands<String> cmd = conn.async();
            RedisFuture[] futures = new RedisFuture[4 * jobs.size()];
            cmd.setAutoFlushCommands(false);
            int i = 0;
            for (JobMetadata job : jobs) {
                String jobId = job.getJobId().toString();
                futures[i++] = cmd.hmset(key(job.getJobId()), map(job));
                futures[i++] = cmd.sadd(index(jobIdName, "all"), jobId);
                futures[i++] = cmd.sadd(index(jobStatusName, job.getJobStatus()), jobId);
                futures[i++] = cmd.sadd(index(clusterIdName, job.getClusterId()), jobId);
            }
            cmd.flushCommands();
            await(futures);
        }
    }

    @Override
    public void deleteJobMetadata(String jobId) throws IOException, JobNotFoundException {
        log.info("Deleting job with ID [{}]", jobId);
        deletedAccessor.putDeletedJobMetadata(performDeleteJob(jobId));
    }

    @Override
    public List<JobMetadata> getJobMetadataList() throws IOException {
        log.info("Getting job metadata list");
        try (RedisConnection<String> conn = connect()) {
            return getJobMetadata(conn.sync().smembers(index(jobIdName, "all")));
        }
    }

    @Override
    public List<JobMetadata> getRunningJobs() throws IOException {
        log.info("Getting list of running jobs");
        try (RedisConnection<String> conn = connect()) {
            return getJobMetadata(conn.sync().smembers(index(jobStatusName, JobStatus.RUNNING.getValue())));
        }
    }

    @Override
    public List<JobMetadata> getJobsAssociatedWithCluster(String clusterId) throws IOException {
        log.info("Getting jobs associated with cluster [{}]", clusterId);
        try (RedisConnection<String> conn = connect()) {
            return getJobMetadata(conn.sync().smembers(index(clusterIdName, clusterId)));
        }
    }

    @Override
    public List<JobMetadata> getRunningJobsAssociatedWithCluster(String clusterId) throws IOException {
        log.info("Getting running jobs associated with cluster [{}]", clusterId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> running = cmd.smembers(index(jobStatusName, JobStatus.RUNNING.getValue()));
            RedisFuture<Set<String>> cluster = cmd.smembers(index(clusterIdName, clusterId));
            cmd.flushCommands();
            await(running, cluster);
            Set<String> ids = running.get();
            ids.retainAll(cluster.get());
            return getJobMetadata(ids);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while getting jobs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void deleteDebugJobs() throws IOException {
        log.info("Deleting all DEBUG jobs");
        try (RedisConnection<String> conn = connect()) {
            performDeleteJob(conn.sync().smembers(index(jobStatusName, "DEBUG")));
        }
    }

    @Override
    public void deleteJobs(Set<String> jobIds) throws IOException {
        log.info("Performing bulk delete of [{}] jobs", jobIds);
        deletedAccessor.putDeletedJobMetadata(performDeleteJob(jobIds));
    }

    @Override
    public void deleteEmailFromJobs(EmailMetaData emailMetaData)
        throws IOException {
        String emailId = emailMetaData.getEmailId();
        log.info("Deleting [{}] from all related jobs", emailId);
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<Set<String>> jobIds = cmd.smembers(index(DatabaseConstants.INDEX_EMAILID_JOBID, emailId));
            cmd.flushCommands();
            await(jobIds);
            List<JobMetadata> jobList = getJobMetadata(jobIds.get());
            for (JobMetadata jobMetadata : jobList) {
                if (jobMetadata.getOwnerEmail() != null && !jobMetadata.getOwnerEmail().isEmpty()) {
                    List<String> emails = Arrays.stream(jobMetadata.getOwnerEmail().split(Constants.COMMA_DELIMITER))
                        .collect(Collectors.toList());
                    emails.remove(emailId);
                    jobMetadata.setOwnerEmail(emails.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER)));
                }
            }
            putJobMetadata(jobList);
            cmd.setAutoFlushCommands(false);
            RedisFuture<Long> deleteIndex = cmd.del(index(DatabaseConstants.INDEX_EMAILID_JOBID, emailId));
            cmd.flushCommands();
            await(deleteIndex);
            emailMetadataAccessor.deleteEmailMetadata(emailMetaData);
            log.info("Successfully deleted email {} from all related jobs", emailId);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Error while deleting emails for jobs!", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public String saveRedisJobsMetadata() throws IOException {
        log.info("Saving redis snapshot");
        String response;
        try (RedisConnection<String> conn = connect()) {
            AsyncCommands<String> cmd = conn.async();
            cmd.setAutoFlushCommands(false);
            RedisFuture<String> res = cmd.bgsave();
            cmd.flushCommands();
            await(res);
            response = res.get();
            log.info("Status: " + response);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Exception while running BGSAVE", e);
            throw new IOException(e.getMessage(), e);
        }
        return response;
    }

}
