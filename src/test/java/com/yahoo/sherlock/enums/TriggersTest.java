/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.enums;

import com.yahoo.sherlock.settings.Constants;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TriggersTest {

    @Test
    public void testTriggersToString() {
        Triggers[] triggers = {Triggers.DAY, Triggers.HOUR, Triggers.MONTH, Triggers.WEEK};
        String[] expected = {"day", "hour", "month", "week"};
        for (int i = 0; i < triggers.length; i++) {
            assertEquals(triggers[i].toString(), expected[i]);
        }
    }

    @Test
    public void testGetAllValues() {
        assertEquals(Triggers.getAllValues().size(), 4);
    }

    /**
     * Test toString() method.
     * @throws Exception exception
     */
    @Test
    public void testToString() throws Exception {
        Assert.assertEquals(Triggers.HOUR.toString(), Constants.HOUR.toLowerCase());
    }

}
