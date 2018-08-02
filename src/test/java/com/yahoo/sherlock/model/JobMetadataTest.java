/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class JobMetadataTest {

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
        JobMetadata m = new JobMetadata(
                1, "b", "c", "d", "e", "f",
                "g", "h", "i", 123, 1234,
                "m", 4, 1, "n", 3.0, 2, 12, "ts", "ad");
        assertEquals(m.getJobId(), (Integer) 1);
        assertEquals(m.getClusterId(), (Integer) 2);
        assertEquals(m.getOwner(), "b");
        assertEquals(m.getOwnerEmail(), "c");
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
        assertEquals(m.getSigmaThreshold(), 3.0);
        assertEquals(m.getTimeseriesModel(), "ts");
        assertEquals(m.getAnomalyDetectionModel(), "ad");
    }

}
