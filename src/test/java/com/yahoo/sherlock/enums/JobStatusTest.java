/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class JobStatusTest {

    @Test
    public void testJobStatusToString() {
        assertEquals(JobStatus.RUNNING.toString(), "running");
        assertEquals(JobStatus.ERROR.toString(), "error");
        assertEquals(JobStatus.CREATED.toString(), "created");
        assertEquals(JobStatus.STOPPED.toString(), "stopped");
        assertEquals(JobStatus.ZOMBIE.toString(), "zombie");
    }

    /**
     * Tests the getValue function.
     */
    @Test
    public void testGetValue() {
        assertEquals(JobStatus.RUNNING.getValue(), "running".toUpperCase());
        assertEquals(JobStatus.CREATED.getValue(), "created".toUpperCase());
        assertEquals(JobStatus.STOPPED.getValue(), "stopped".toUpperCase());
        assertEquals(JobStatus.ERROR.getValue(), "error".toUpperCase());
        assertEquals(JobStatus.ZOMBIE.getValue(), "zombie".toUpperCase());
    }

    /**
     * Tests the toString function.
     */
    @Test
    public void testToString() {
        assertEquals(JobStatus.RUNNING.toString(), "running");
        assertEquals(JobStatus.CREATED.toString(), "created");
        assertEquals(JobStatus.STOPPED.toString(), "stopped");
        assertEquals(JobStatus.ERROR.toString(), "error");
        assertEquals(JobStatus.ZOMBIE.toString(), "zombie");
    }

}
