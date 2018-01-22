/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class JsonTimelineTest {

    @Test
    public void testJsonTimelinePoint() {
        JsonTimeline.TimelinePoint point = new JsonTimeline.TimelinePoint();
        String timestamp = "1234";
        point.setTimestamp(timestamp);
        assertEquals(point.getTimestamp(), timestamp);
        String type = "type";
        point.setType(type);
        assertEquals(point.getType(), type);
    }

    @Test
    public void testJsonTimelineEqualsAndHash() {
        JsonTimeline.TimelinePoint point = new JsonTimeline.TimelinePoint();
        point.setTimestamp("1234");
        point.setType("type");
        assertTrue(point.equals(point));
        assertFalse(point.equals(null));
        assertFalse(point.equals("string"));
        JsonTimeline.TimelinePoint point1 = new JsonTimeline.TimelinePoint();
        point1.setTimestamp("12345");
        assertFalse(point.equals(point1));
        point1.setTimestamp("1234");
        point1.setType("type");
        assertTrue(point.equals(point1));
        assertEquals(point1.hashCode(), point.hashCode());
    }

    @Test
    public void testJsonTimelineFields() {
        JsonTimeline timeline = new JsonTimeline();
        timeline.setFrequency("day");
        assertEquals(timeline.getFrequency(), "day");
        List<JsonTimeline.TimelinePoint> points = new ArrayList<>();
        points.add(new JsonTimeline.TimelinePoint());
        timeline.setTimelinePoints(points);
        assertEquals(timeline.getTimelinePoints(), points);
    }

}
