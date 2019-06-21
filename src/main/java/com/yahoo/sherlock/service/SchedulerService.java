/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.scheduler.BackupTask;
import com.yahoo.sherlock.scheduler.EmailSenderTask;
import com.yahoo.sherlock.scheduler.ExecutionTask;
import com.yahoo.sherlock.scheduler.RecoverableThreadScheduler;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.TimeUtils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is responsible for handling the scheduling, rescheduling,
 * checking, and retrieving of running jobs. New jobs or restarted
 * jobs are assigned a next run time using the {@code Scheduler}
 * instance and are rescheduled with it. This class will call the
 * database queue handler.
 */
@Slf4j
public class SchedulerService {

    /**
     * The singleton instance of this class.
     */
    private static SchedulerService schedulerService;

    /**
     * Class job execution service instance.
     */
    private JobExecutionService jobExecutionService;

    /**
     * ScheduledExecutorService interface for pinging the backend priority queue.
     */
    private RecoverableThreadScheduler recoverableThreadScheduler;

    /**
     * Class job scheduler instance that communicates with the
     * backend task queue.
     */
    private JobScheduler jobScheduler;

    /**
     * Class execution task instance.
     */
    private ExecutionTask executionTask;

    /**
     * Class backup task instance.
     */
    private BackupTask backupTask;

    /**
     * Class email sender task instance.
     */
    private EmailSenderTask emailSenderTask;

    /**
     * Private singleton constructor.
     */
    private SchedulerService() {
        jobExecutionService = new JobExecutionService();
        jobScheduler = Store.getJobScheduler();
        recoverableThreadScheduler = null;
        executionTask = null;
        backupTask = null;
        emailSenderTask = null;
    }

    /**
     * Get the single instance of this class.
     *
     * @return schedulerService the single instance
     */
    public static SchedulerService getInstance() {
        if (schedulerService == null) {
            schedulerService = new SchedulerService();
        }
        return schedulerService;
    }

    /**
     * Create the recoverableThreadScheduler instance.
     */
    public void instantiateMasterScheduler() {
        log.info("Instantiating timer instance");
        if (recoverableThreadScheduler != null) {
            log.info("Timer is already instantiated");
            return;
        }
        recoverableThreadScheduler = new RecoverableThreadScheduler(6);
    }

    /**
     * Start the execution task.
     */
    public void startMasterScheduler() {
        log.info("Starting execution task");
        if (executionTask != null) {
            log.info("Execution task has already been started");
            return;
        }
        if (recoverableThreadScheduler == null) {
            instantiateMasterScheduler();
        }
        executionTask = new ExecutionTask(
                jobExecutionService,
                this,
                jobScheduler,
                Store.getJobMetadataAccessor()
        );
        int period = CLISettings.EXECUTION_DELAY;
        int delay = 0;
        recoverableThreadScheduler.scheduleAtFixedRate(executionTask, delay, period, TimeUnit.SECONDS);
    }

    /**
     * Start the redis db backup task.
     */
    public void startEmailSenderScheduler() {
        log.info("Starting email sender task");
        if (emailSenderTask != null) {
            log.info("Email sender task has already been started");
            return;
        }
        if (recoverableThreadScheduler == null) {
            instantiateMasterScheduler();
        }
        emailSenderTask = new EmailSenderTask();
        int period = Constants.SECONDS_IN_MINUTE;
        int delay = 10;
        recoverableThreadScheduler.scheduleAtFixedRate(emailSenderTask, delay, period, TimeUnit.SECONDS);
    }

    /**
     * Start the redis db backup task.
     */
    public void startBackupScheduler() {
        log.info("Starting backup task");
        if (backupTask != null) {
            log.info("Backup task has already been started");
            return;
        }
        if (recoverableThreadScheduler == null) {
            instantiateMasterScheduler();
        }
        backupTask = new BackupTask();
        int period = Constants.SECONDS_IN_MINUTE;
        int delay = 20;
        recoverableThreadScheduler.scheduleAtFixedRate(backupTask, delay, period, TimeUnit.SECONDS);
    }

    /**
     * Stop the execution task.
     */
    public void shutdownMasterScheduler() {
        if (executionTask == null) {
            log.info("Execution task already stopped");
            return;
        }
        executionTask = null;
        recoverableThreadScheduler.shutdown();
    }

    /**
     * Destroy the execution task and the timer.
     */
    public void destroyMasterScheduler() {
        if (executionTask != null) {
            recoverableThreadScheduler.shutdown();
            executionTask = null;
        }
        if (recoverableThreadScheduler != null) {
            recoverableThreadScheduler.shutdown();
            recoverableThreadScheduler = null;
        }
    }

    /**
     * Schedule a job. This method will assign a next run time and
     * place the job in the backend priority queue.
     *
     * @param jobMetadata job metadata object to schedule
     * @throws SchedulerException if an error occurs while scheduling the job
     */
    public void scheduleJob(JobMetadata jobMetadata) throws SchedulerException {
        Pair<Integer, Integer> nextTimes = jobScheduleTime(jobMetadata);
        Integer nextQueryTime = nextTimes.getLeft();
        Integer nextRunTime = nextTimes.getRight();
        jobMetadata.setEffectiveQueryTime(nextQueryTime);
        jobMetadata.setEffectiveRunTime(nextRunTime);
        try {
            jobScheduler.pushQueue(nextRunTime, jobMetadata.getJobId().toString());
        } catch (IOException e) {
            log.error("Error while adding job to queue", e);
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    /**
     * Reschedule the job. This method will assign
     * the next run time and push to the queue.
     *
     * @param jobMetadata job to schedule
     * @throws SchedulerException if an error occurs while scheduling the job
     */
    public void rescheduleJob(JobMetadata jobMetadata) throws SchedulerException {
        if (!jobMetadata.getJobStatus().equals(JobStatus.ERROR.getValue())) {
            Pair<Integer, Integer> nextTimes = jobRescheduleTime(jobMetadata);
            Integer nextQueryTime = nextTimes.getLeft();
            Integer nextRunTime = nextTimes.getRight();
            jobMetadata.setEffectiveQueryTime(nextQueryTime);
            jobMetadata.setEffectiveRunTime(nextRunTime);
            try {
                jobScheduler.pushQueue(nextRunTime, jobMetadata.getJobId().toString());
            } catch (IOException e) {
                log.error("Error while adding job to queue", e);
                throw new SchedulerException(e.getMessage(), e);
            }
        }
    }

    /**
     * Stop a running job. This method will call the job scheduler
     * to remove the job from the task queue.
     *
     * @param jobId ID of the job to stop
     * @throws SchedulerException if an error occurs while unscheduling the job
     */
    public void stopJob(Integer jobId) throws SchedulerException {
        try {
            jobScheduler.removeQueue(jobId);
            jobScheduler.removePending(jobId);
        } catch (IOException e) {
            log.error("Error while unscheduling job", e);
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    /**
     * Stop all selected running job. This method will call the job scheduler
     * to remove jobs from the task queue.
     *
     * @param jobIds IDs of jobs to stop
     * @throws SchedulerException if an error occurs while unscheduling the job
     */
    public void stopJob(Set<String> jobIds) throws SchedulerException {
        try {
            jobScheduler.removeQueue(jobIds);
            jobScheduler.removePending(jobIds);
        } catch (IOException e) {
            log.error("Error while unscheduling jobs", e);
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    /**
     * Stop all jobs.
     * @throws SchedulerException if an error occurs while unscheduling the job
     */
    public void removeAllJobsFromQueue() throws SchedulerException {
        try {
            jobScheduler.removeAllQueue();
        } catch (IOException e) {
            log.error("Error while unscheduling all jobs", e);
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    /**
     * Unschedule all jobs in a list and reschedule them with new times.
     *
     * @param jobs a list of jobs to unschedule and then reschedule
     *             with newly generated times
     * @throws SchedulerException if an error occurs while scheduling the jobs
     */
    public void stopAndReschedule(List<JobMetadata> jobs) throws SchedulerException {
        log.info("Stopping and then rescheduling [{}] jobs", jobs.size());
        List<String> jobIds = new ArrayList<>(jobs.size());
        List<Pair<Integer, String>> jobsAndTimes = new ArrayList<>(jobs.size());
        for (JobMetadata job : jobs) {
            if (job.getJobStatus().equals(JobStatus.NODATA.getValue()) ||
                job.getJobStatus().equals(JobStatus.RUNNING.getValue())) {
                jobIds.add(job.getJobId().toString());
                Pair<Integer, Integer> nextTimes = jobScheduleTime(job);
                Integer nextQueryTime = nextTimes.getLeft();
                Integer nextRunTime = nextTimes.getRight();
                job.setEffectiveQueryTime(nextQueryTime);
                job.setEffectiveRunTime(nextRunTime);
                jobsAndTimes.add(new ImmutablePair<>(nextRunTime, job.getJobId().toString()));
            }
        }
        try {
            jobScheduler.removeQueue(jobIds);
            jobScheduler.pushQueue(jobsAndTimes);
        } catch (IOException e) {
            log.error("Error while rescheduling jobs!", e);
            throw new SchedulerException(e.getMessage(), e);
        }
    }

    /**
     * Return an execution time in Unix timestamp minutes
     * based on the supplied job. This method should stagger
     * the execution time over an hour.
     *
     * @param job the job for which to get an execution time
     * @return timestamp in minutes
     */
    public Pair<Integer, Integer> jobScheduleTime(JobMetadata job) {
        Integer hoursOfLag = job.getHoursOfLag();
        Granularity granularity = Granularity.getValue(job.getFrequency());
        if (granularity == null) {
            log.error(
                    "Job [{}] passed with unknown frequency [{}], default to 'day'",
                    job.getJobId(),
                    job.getFrequency()
            );
            granularity = Granularity.DAY;
        }
        // Take the current time, subtract the hours of lag
        // and then floor to the nearest granularity
        Integer effectiveQueryTime = granularity.getEndTimeForInterval(ZonedDateTime.now(ZoneOffset.UTC).minusHours(hoursOfLag));
        // Obtain a hash value between 0 and 60 (minutes) to offset the job
        int idInt = job.getJobId() == null ? 30 : job.getJobId();
        // Return the effective run time as the effective query time plus
        // the hours of lag (in minutes)
        Integer offset = job.getFrequency().equalsIgnoreCase(Triggers.MINUTE.toString()) ? Triggers.MINUTE.getMinutes() : Math.abs(idInt) % Constants.MINUTES_IN_HOUR;
        Integer effectiveRunTime = effectiveQueryTime + hoursOfLag * 60 + offset;
        return new ImmutablePair<>(effectiveQueryTime, effectiveRunTime);
    }

    /**
     * Reschedule a task. This method will schedule a task
     * at a constant time from its previous execution time.
     * If the task does not have a previous execution time,
     * then it is assigned a new one.
     *
     * @param job to job for which to get a reschedule time
     * @return timestamp in minutes
     */
    public Pair<Integer, Integer> jobRescheduleTime(JobMetadata job) {
        Integer lastRunTime = job.getEffectiveRunTime();
        if (lastRunTime == null) {
            return jobScheduleTime(job);
        }
        Granularity granularity = Granularity.getValue(job.getFrequency());
        if (granularity == null) {
            granularity = Granularity.DAY;
        }
        Integer rescheduleTime;
        Integer rescheduleQueryTime;
        // update query interval time and next schedule time for next job execution
        if (Constants.MONTH.equalsIgnoreCase(granularity.toString())) {
            rescheduleTime = TimeUtils.addMonth(lastRunTime, 1);
            rescheduleQueryTime = TimeUtils.addMonth(job.getEffectiveQueryTime(), 1);
        } else {
            rescheduleTime = lastRunTime + granularity.getMinutes();
            rescheduleQueryTime = job.getEffectiveQueryTime() + granularity.getMinutes();
        }
        return new ImmutablePair<>(rescheduleQueryTime, rescheduleTime);
    }

}
