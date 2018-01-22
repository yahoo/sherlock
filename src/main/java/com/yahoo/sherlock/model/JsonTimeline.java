/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.model;

import java.util.List;

import lombok.Data;

/**
 * Helper class to generate the report timeline.
 */
@Data
public class JsonTimeline {

    /** List for timeline(calender view on UI) points. **/
    private List<TimelinePoint> timelinePoints;

    /** Cron job frequency. **/
    private String frequency;

    /**
     * Inner class for timeline point.
     */
    @Data
    public static class TimelinePoint {

        /**
         * Timestamp related to the timeline point(square box on UI).
         */
        private String timestamp;

        /**
         * Type of the timeline point.
         * type can be 'Anomaly', 'No anomaly', 'Error' or 'No scheduled Job'
         */
        private String type;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null) {
                return false;
            }
            if (!(o instanceof TimelinePoint)) {
                return false;
            }
            TimelinePoint that = (TimelinePoint) o;
            if (!timestamp.equals(that.timestamp)) {
                return false;
            }
            return type.equals(that.type);
        }

        @Override
        public int hashCode() {
            int result = timestamp.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }
}
