/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class JobMetadataTest {

    public JobMetadata utilityMethod() {
        JobMetadata jobMetadata = new JobMetadata();
        jobMetadata.setJobId(1);
        jobMetadata.setOwner("b");
        jobMetadata.setOwnerEmail("c");
        jobMetadata.setEmailOnNoData(false);
        jobMetadata.setUserQuery("d");
        jobMetadata.setQuery("e");
        jobMetadata.setTestName("f");
        jobMetadata.setTestDescription("g");
        jobMetadata.setUrl("h");
        jobMetadata.setJobStatus("i");
        jobMetadata.setEffectiveRunTime(123);
        jobMetadata.setEffectiveQueryTime(1234);
        jobMetadata.setGranularity("m");
        jobMetadata.setTimeseriesRange(4);
        jobMetadata.setGranularityRange(1);
        jobMetadata.setFrequency("n");
        jobMetadata.setSigmaThreshold(3.0);
        jobMetadata.setTimeseriesModel("ts");
        jobMetadata.setAnomalyDetectionModel("ad");
        jobMetadata.setClusterId(2);
        return jobMetadata;
    }

    @Test
    public void testEqualsAndHashCode() {
        JobMetadata m = new JobMetadata();
        Set<JobMetadata> sjm = Collections.singleton(m);
        assertEquals(m, sjm.iterator().next());
        m.setJobId(5);
        assertEquals(m.hashCode(), ((Integer) 5).hashCode());
        assertFalse(m.equals(null));
        assertFalse(m.equals("5"));
        JobMetadata m1 = new JobMetadata();
        m1.setJobId(5);
        assertEquals(m, m1);
    }

    @Test
    public void testParameterConstructor() {
        JobMetadata m = new JobMetadata(utilityMethod());
        assertEquals(m.getJobId(), (Integer) 1);
        assertEquals(m.getClusterId(), (Integer) 2);
        assertEquals(m.getOwner(), "b");
        assertEquals(m.getOwnerEmail(), "c");
        assertFalse(m.getEmailOnNoData());
        assertEquals(m.getUserQuery(), "d");
        assertEquals(m.getQuery(), "e");
        assertEquals(m.getTestName(), "f");
        assertEquals(m.getTestDescription(), "g");
        assertEquals(m.getUrl(), "h");
        assertEquals(m.getJobStatus(), "i");
        assertEquals(m.getEffectiveRunTime(), (Integer) 123);
        assertEquals(m.getEffectiveQueryTime(), (Integer) 1234);
        assertEquals(m.getGranularity(), "m");
        assertEquals(m.getTimeseriesRange(), (Integer) 4);
        assertEquals(m.getGranularityRange(), (Integer) 1);
        assertEquals(m.getFrequency(), "n");
        assertEquals(m.getSigmaThreshold(), Double.valueOf(3.0));
        assertEquals(m.getTimeseriesModel(), "ts");
        assertEquals(m.getAnomalyDetectionModel(), "ad");
    }

    @Test
    public void testClone() {
        JobMetadata m = new JobMetadata(utilityMethod());
        JobMetadata mCloned = new JobMetadata();
        try {
            mCloned = (JobMetadata) m.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        assertEquals(mCloned.getJobId(), (Integer) 1);
        assertEquals(mCloned.getClusterId(), (Integer) 2);
        assertEquals(mCloned.getOwner(), "b");
        assertEquals(mCloned.getOwnerEmail(), "c");
        assertFalse(mCloned.getEmailOnNoData());
        assertEquals(mCloned.getUserQuery(), "d");
        assertEquals(mCloned.getQuery(), "e");
        assertEquals(mCloned.getTestName(), "f");
        assertEquals(mCloned.getTestDescription(), "g");
        assertEquals(mCloned.getUrl(), "h");
        assertEquals(mCloned.getJobStatus(), "i");
        assertEquals(mCloned.getEffectiveRunTime(), (Integer) 123);
        assertEquals(mCloned.getEffectiveQueryTime(), (Integer) 1234);
        assertEquals(mCloned.getGranularity(), "m");
        assertEquals(mCloned.getTimeseriesRange(), (Integer) 4);
        assertEquals(mCloned.getGranularityRange(), (Integer) 1);
        assertEquals(mCloned.getFrequency(), "n");
        assertEquals(mCloned.getSigmaThreshold(), Double.valueOf(3.0));
        assertEquals(mCloned.getTimeseriesModel(), "ts");
        assertEquals(mCloned.getAnomalyDetectionModel(), "ad");
    }

    @Test
    public void testCopyJob() {
        JobMetadata m = new JobMetadata(utilityMethod());
        JobMetadata mCopy = JobMetadata.copyJob(m);
        assertEquals(mCopy.getJobId(), (Integer) 1);
        assertEquals(mCopy.getClusterId(), (Integer) 2);
        assertEquals(mCopy.getOwner(), "b");
        assertEquals(mCopy.getOwnerEmail(), "c");
        assertFalse(mCopy.getEmailOnNoData());
        assertEquals(mCopy.getUserQuery(), "d");
        assertEquals(mCopy.getQuery(), "e");
        assertEquals(mCopy.getTestName(), "f");
        assertEquals(mCopy.getTestDescription(), "g");
        assertEquals(mCopy.getUrl(), "h");
        assertEquals(mCopy.getJobStatus(), "i");
        assertNull(mCopy.getEffectiveRunTime());
        assertNull(mCopy.getEffectiveQueryTime());
        assertEquals(mCopy.getGranularity(), "m");
        assertEquals(mCopy.getTimeseriesRange(), (Integer) 4);
        assertEquals(mCopy.getGranularityRange(), (Integer) 1);
        assertEquals(mCopy.getFrequency(), "n");
        assertEquals(mCopy.getSigmaThreshold(), Double.valueOf(3.0));
        assertEquals(mCopy.getTimeseriesModel(), "ts");
        assertEquals(mCopy.getAnomalyDetectionModel(), "ad");
    }

    @Test
    public void testUpdate() {
        JobMetadata j1 = new JobMetadata(utilityMethod());
        JobMetadata j2 = new JobMetadata(utilityMethod());
        j2.setHoursOfLag(29);
        Assert.assertNull(j1.getHoursOfLag());
        j1.update(j2);
        Assert.assertEquals(j1.getHoursOfLag(), (Integer) 29);
    }

    @Test
    public void testIsScheduleChangeRequire() {
        JobMetadata j = new JobMetadata(utilityMethod());
        UserQuery u = UserQueryTest.getUserQuery();
        j.setHoursOfLag(22);
        u.setHoursOfLag(22);
        j.setGranularity("hour");
        u.setGranularity("hour");
        j.setFrequency("hour");
        u.setFrequency("hour");
        j.setClusterId(1);
        u.setClusterId(1);
        assertFalse(j.isScheduleChangeRequire(u));
        j.setClusterId(1);
        u.setClusterId(2);
        assertTrue(j.isScheduleChangeRequire(u));
        u.setClusterId(1);
        j.setFrequency("day");
        u.setFrequency("hour");
        assertTrue(j.isScheduleChangeRequire(u));
        j.setFrequency("hour");
        j.setGranularity("day");
        u.setGranularity("hour");
        assertTrue(j.isScheduleChangeRequire(u));
        j.setGranularity("hour");
        j.setHoursOfLag(23);
        u.setHoursOfLag(22);
        assertTrue(j.isScheduleChangeRequire(u));
        j.setHoursOfLag(22);
        assertFalse(j.isScheduleChangeRequire(u));
    }
}
