/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class StoreTest {

    @Test
    public void testAccessorTypeOrdinal() {
        assertEquals(0, Store.AccessorType.ANOMALY_REPORT.ordinal());
        assertEquals(3, Store.AccessorType.JOB_METADATA.ordinal());
    }

}
