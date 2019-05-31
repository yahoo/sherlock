/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.enums.Granularity;
import com.yahoo.sherlock.enums.JobStatus;
import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.exception.SchedulerException;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.utils.BackupUtils;
import com.yahoo.sherlock.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * ScheduledExecutorService which polls the backend task queue for
 * any pending tasks that need to be ran and then
 * forwards them to the job execution service.
 */
@Slf4j
public class ExecutionTask implements Runnable {

    /**
     * Job execution service instance, which
     * executes jobs retrieved from the scheduler.
     */
    private final JobExecutionService jobExecutionService;
    /**
     * Scheduler service instance, which is used
     * to reschedule a job once ran.
     */
    private final SchedulerService schedulerService;
    /**
     * Job scheduler instance which is used to
     * retrieve pending jobs.
     */
    private final JobScheduler jobScheduler;
    /**
     * Job metadata accessor class to update jobs
     * that have been ran.
     */
    private final JobMetadataAccessor jobMetadataAccessor;
    /**
     * Email Service obj to send emails.
     */
    private EmailService emailService;

    /**
     * Create a new execution task.
     *
     * @param jobExecutionService job execution service instance to use
     * @param schedulerService    scheduler service to use
     * @param jobScheduler        job scheduler to use
     * @param jobMetadataAccessor job accessor instance to use
     */
    public ExecutionTask(
            JobExecutionService jobExecutionService,
            SchedulerService schedulerService,
            JobScheduler jobScheduler,
            JobMetadataAccessor jobMetadataAccessor
    ) {
        this.jobExecutionService = jobExecutionService;
        this.schedulerService = schedulerService;
        this.jobScheduler = jobScheduler;
        this.jobMetadataAccessor = jobMetadataAccessor;
        this.emailService = new EmailService();
    }

    /**
     * Run the task, which grabs the current time in minutes
     * and starts consuming pending tasks.
     */
    @Override
    public void run() {
        long seconds = TimeUtils.getTimestampSeconds();
        try {
            consumeAndExecuteTasks(seconds / 60L);
            runEmailSender(seconds / 60L);
            backupRedisDB(seconds);
        } catch (Exception e) {
            log.error("Error while running job", e);
        }
    }

    /**
     * This method will attempt to determine whether a job
     * is lagging behind. For example, if there were no workers
     * available to execute jobs.
     *
     * @param job              the job to analyze
     * @param timestampMinutes the current time in minutes
     * @return true if the job is lagging several executions
     */
    public static boolean isLaggingJob(JobMetadata job, long timestampMinutes) {
        Integer effectiveRunTime = job.getEffectiveRunTime();
        // Get minutes of job frequency period
        Granularity granularity = Granularity.getValue(job.getFrequency());
        // Job will rescheduled for `effectiveRunTime` + `granularity.getMinutes()`
        int expectedNextRunTime = effectiveRunTime + granularity.getMinutes();
        // If the current time is greater than the expected next run time
        // job will run again after being rescheduled
        return timestampMinutes > expectedNextRunTime;
    }

    /**
     * Given the current time in minutes, pop tasks from the
     * queue and execute them, then reschedule them.
     *
     * @param timestampMinutes the current time in minutes
     * @throws IOException        if an error retrieving the job occurs
     * @throws SchedulerException if an error rescheduling the job occurs
     */
    private void consumeAndExecuteTasks(long timestampMinutes) throws IOException, SchedulerException {
        JobMetadata jobMetadata;
        // CRITICAL REGION: please verify very carefully if you make change to this part
        log.info("Execution task ping for time " + TimeUtils.getTimeFromSeconds(timestampMinutes * 60L, Constants.TIMESTAMP_FORMAT_NO_SECONDS));
        while ((jobMetadata = jobScheduler.popQueue(timestampMinutes)) != null) {
            if (isLaggingJob(jobMetadata, timestampMinutes)) {
                // Perform a backfill instead and schedule for next start time
                jobExecutionService.backfillJobFromIntervalEnd(jobMetadata);
                // The run time that the job would have had if it was executed normally
                Pair<Integer, Integer> nextTimes = schedulerService.jobScheduleTime(jobMetadata);
                Integer nextQueryTime = nextTimes.getLeft();
                Integer nextRunTime = nextTimes.getRight();
                if (nextRunTime <= timestampMinutes) {
                    int offset = Granularity.getValue(jobMetadata.getFrequency()).getMinutes();
                    nextQueryTime += offset;
                    nextRunTime += offset;
                }
                // If the next runtime still less than current time
                // Terminate the job as "ZOMBIE" job
                if (nextRunTime <= timestampMinutes) {
                    jobMetadata.setJobStatus(JobStatus.ZOMBIE.getValue());
                } else {
                    jobMetadata.setEffectiveQueryTime(nextQueryTime);
                    jobMetadata.setEffectiveRunTime(nextRunTime);
                    jobScheduler.pushQueue(nextRunTime, jobMetadata.getJobId().toString());
                }
            } else {
                // Perform regular job execution and schedule for next time
                jobExecutionService.execute(jobMetadata);
                schedulerService.rescheduleJob(jobMetadata);
            }
            jobMetadataAccessor.putJobMetadata(jobMetadata);
            jobScheduler.removePending(jobMetadata.getJobId());
        }
    }

    /**
     * Method to send email if required at this time.
     * @param timestampMinutes input current timestamp in minutes
     * @throws IOException     if an error sending email
     */
    public void runEmailSender(long timestampMinutes) throws IOException {
        ZonedDateTime date = TimeUtils.zonedDateTimeFromMinutes(timestampMinutes);
        emailService.sendConsolidatedEmail(date, Triggers.DAY.toString());
        emailService.sendConsolidatedEmail(date, Triggers.HOUR.toString());
        if (date.getDayOfMonth() == 1) {
            emailService.sendConsolidatedEmail(date, Triggers.MONTH.toString());
        } else if (date.getDayOfWeek().getValue() == 1) {
            emailService.sendConsolidatedEmail(date, Triggers.WEEK.toString());
        }
    }

    /**
     * Method to backup redis data as redis local dump and (as json dump if specified).
     * @param timestamp    ping timestamp of execution task thread
     * @throws IOException exception
     */
    public void backupRedisDB(long timestamp) throws IOException {
        ZonedDateTime date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneOffset.UTC);
        // save redis snapshot
        if (date.getMinute() == 0 && date.getHour() == 0 && date.getSecond() < CLISettings.EXECUTION_DELAY) {
            jobMetadataAccessor.saveRedisJobsMetadata();
            // save redis data as json file if path is specified
            if (CLISettings.BACKUP_REDIS_DB_PATH != null) {
                BackupUtils.startBackup();
            }
        }
    }

}
