/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.store.JobMetadataAccessor;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Test for {@code BackupTask}.
 */
public class BackupTaskTest {

    private BackupTask backupTask;
    private JobMetadataAccessor jobMetadataAccessor;

    @BeforeMethod
    public void setUp() throws Exception {
        backupTask = mock(BackupTask.class);
        jobMetadataAccessor = mock(JobMetadataAccessor.class);
    }

    @Test
    public void testRun() throws Exception {
        doNothing().when(backupTask).backupRedisDB(anyLong());
        doCallRealMethod().when(backupTask).run();
        backupTask.run();
        Mockito.verify(backupTask, Mockito.times(1)).backupRedisDB(anyLong());
    }

    @Test(expectedExceptions = IOException.class)
    public void testBackupRedisDBException() throws Exception {
        backupTask = new BackupTask();
        TestUtilities.inject(backupTask, "jobMetadataAccessor", jobMetadataAccessor);
        doThrow(new IOException()).when(jobMetadataAccessor).saveRedisJobsMetadata();
        backupTask.backupRedisDB(1560384000 / 60L);
    }

}
