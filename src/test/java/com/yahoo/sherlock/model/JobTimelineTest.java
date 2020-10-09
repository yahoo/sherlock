/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import com.yahoo.sherlock.settings.CLISettingsTest;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.enums.Granularity;

import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertTrue;

public class JobTimelineTest {

    @Test
    public void testPoint() {
        JobTimeline.Point g = new JobTimeline.Point();
        assertEquals(g.getDisplay(), "false");
        g = new JobTimeline.Point(100);
        assertTrue(g.getTimestamp() == g.getEndTimestamp());
        assertEquals(g.getTimestamp(), 100 * 1000);
    }

    @Test
    public void testSeries() {
        JobTimeline.Series s = new JobTimeline.Series();
        assertNull(s.getCls());
        s = new JobTimeline.Series(new JobTimeline.Point[3], "label", 1, "desc");
        assertEquals(s.getTimes().length, 3);
        assertEquals(s.getLabel(), "label");
        assertEquals(s.getJobId(), "1");
        assertEquals(s.getDescription(), "desc");
    }

    private static JobMetadata job(int id, long time) {
        JobMetadata j = new JobMetadata();
        j.setTestName("name" + id);
        j.setTestDescription("desc");
        j.setJobId(id);
        j.setEffectiveRunTime((int) time);
        j.setGranularity(Granularity.HOUR.name().toLowerCase());
        return j;
    }

    @Test
    public void testGenerateSeries() {
        List<JobMetadata> job = new ArrayList<JobMetadata>() {
            {
                add(job(1, 0));
                add(job(2, 200));
                add(job(3, 300));
            }
        };
        ZonedDateTime start = ZonedDateTime.ofInstant(Instant.ofEpochMilli(130 * 60000), ZoneOffset.UTC);
        ZonedDateTime end = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1050 * 60000), ZoneOffset.UTC);
        int[][] e = {
                {180, 240, 300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900, 960, 1020},
                {200, 260, 320, 380, 440, 500, 560, 620, 680, 740, 800, 860, 920, 980, 1040},
                {300, 360, 420, 480, 540, 600, 660, 720, 780, 840, 900, 960, 1020}
        };
        JobTimeline.Series[] series = JobTimeline.generateSeries(job, start, end);
        assertEquals(job.size(), series.length);
        for (int i = 0; i < 3; i++) {
            assertEquals(series[i].getJobId(), ((Integer) (i + 1)).toString());
            for (int q = 0; q < e[i].length; q++) {
                assertEquals(series[i].getTimes()[q].getTimestamp(), e[i][q] * 60000);
            }
        }
    }

    public static void setField(Field f, Object o, Object v) {
        f.setAccessible(true);
        try {
            f.set(o, v);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Test
    public void testGetCurrentTimeline() throws IOException {
        List<JobMetadata> job = new ArrayList<JobMetadata>() {
            {
                add(job(1, 0));
                add(job(2, 200));
                add(job(3, 300));
            }
        };
        JobScheduler s = mock(JobScheduler.class);
        when(s.getAllQueue()).thenReturn(job);
        JobTimeline jt = new JobTimeline();
        setField(CLISettingsTest.getField("scheduler", JobTimeline.class), jt, s);
        JobTimeline.Series[] series = jt.getCurrentTimeline(Granularity.HOUR);
        assertEquals(3, series.length);
    }

}
