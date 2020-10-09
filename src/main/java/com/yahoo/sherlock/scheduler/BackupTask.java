/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.store.JobMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.utils.BackupUtils;
import com.yahoo.sherlock.utils.TimeUtils;

import java.io.IOException;

import java.time.ZonedDateTime;

import lombok.extern.slf4j.Slf4j;

/**
 * Class for redis data back up runnable task.
 */
@Slf4j
public class BackupTask implements Runnable {

    /** Thread name prefix. */
    private static final String THREAD_NAME_PREFIX = "BackupTask-";

    /**
     * {@code JobMetadataAccessor} instance.
     */
    private JobMetadataAccessor jobMetadataAccessor;

    /**
     * Constructor for initializing.
     */
    public BackupTask() {
        jobMetadataAccessor = Store.getJobMetadataAccessor();
    }

    @Override
    public void run() {
        try {
            String name = THREAD_NAME_PREFIX + Thread.currentThread().getName();
            log.info("Running thread {}", name);
            backupRedisDB(TimeUtils.getTimestampMinutes());
        } catch (IOException e) {
            log.error("Error while running backup task!", e);
        }
    }

    /**
     * Method to backup redis data as redis local dump and (as json dump if specified).
     * @param timestampMinutes ping timestamp (in minutes) of backup task thread
     * @throws IOException exception
     */
    public void backupRedisDB(long timestampMinutes) throws IOException {
        ZonedDateTime date = TimeUtils.zonedDateTimeFromMinutes(timestampMinutes);
        // save redis snapshot
        if (date.getMinute() == 0 && date.getHour() == 0) {
            jobMetadataAccessor.saveRedisJobsMetadata();
            // save redis data as json file if path is specified
            if (CLISettings.BACKUP_REDIS_DB_PATH != null) {
                BackupUtils.startBackup();
            }
        }
    }
}
