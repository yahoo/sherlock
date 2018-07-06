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
import static org.testng.Assert.assertNull;

public class TriggersTest {

    @Test
    public void testTriggersToString() {
        Triggers[] triggers = {Triggers.MINUTE, Triggers.DAY, Triggers.HOUR, Triggers.MONTH, Triggers.WEEK};
        String[] expected = {"minute", "day", "hour", "month", "week"};
        for (int i = 0; i < triggers.length; i++) {
            assertEquals(triggers[i].toString(), expected[i]);
        }
    }

    @Test
    public void testGetAllValues() {
        assertEquals(Triggers.getAllValues().size(), 5);
    }

    /**
     * Test toString() method.
     * @throws Exception exception
     */
    @Test
    public void testToString() throws Exception {
        Assert.assertEquals(Triggers.HOUR.toString(), Constants.HOUR.toLowerCase());
    }

    @Test
    public void testGetMinutes() {
        assertEquals(Triggers.MINUTE.getMinutes(), 1);
        assertEquals(Triggers.HOUR.getMinutes(), 60);
        assertEquals(Triggers.DAY.getMinutes(), 1440);
        assertEquals(Triggers.WEEK.getMinutes(), 10080);
        assertEquals(Triggers.MONTH.getMinutes(), 43800);
    }

    @Test
    public void testGetValue() {
        assertEquals(Triggers.getValue("minute"), Triggers.MINUTE);
        assertEquals(Triggers.getValue("hour"), Triggers.HOUR);
        assertEquals(Triggers.getValue("day"), Triggers.DAY);
        assertEquals(Triggers.getValue("week"), Triggers.WEEK);
        assertEquals(Triggers.getValue("month"), Triggers.MONTH);
        assertNull(Triggers.getValue(null));
        assertNull(Triggers.getValue("blaa"));
    }
}
