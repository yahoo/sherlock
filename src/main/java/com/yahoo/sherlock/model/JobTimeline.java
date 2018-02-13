/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.yahoo.sherlock.store.JobScheduler;
import com.yahoo.sherlock.store.Store;
import com.yahoo.sherlock.enums.Granularity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * The job timeline class represents the scheduled jobs
 * displayed on a timeline for a certain look forward periods.
 */
@Slf4j
public class JobTimeline {

    /**
     * Point class. This class is serialized to JSON
     * and contains the start time and end time.
     */
    @Data
    public static class Point {

        /**
         * The execution time of the job.
         */
        @SerializedName("starting_time")
        private long timestamp;
        /**
         * The 'end time' of the time, which is used
         * to show size on the ui.
         */
        @SerializedName("ending_time")
        private long endTimestamp;
        /**
         * The display mode of the point.
         */
        @SerializedName("display")
        private String display;

        /**
         * Default constructor.
         */
        public Point() {
            display = "false";
        }

        /**
         * Constructor that sets the execution time and
         * an end time.
         *
         * @param timestamp execution time of the job in seconds
         */
        public Point(long timestamp) {
            this();
            this.timestamp = timestamp * 1000L; // convert to millis
            this.endTimestamp = this.timestamp;
            this.display = null;
        }
    }

    /**
     * The series class represents a sequence of points
     * for the job timeline and is serialized to JSON.
     */
    @Data
    public static class Series {
        /**
         * The time points.
         */
        private Point[] times;
        /**
         * The label to use on the timeline, which
         * would be the job name.
         */
        private String label;
        /**
         * The timeline class, which is the same as
         * the label. Used for the stacked timeline.
         */
        @SerializedName("class")
        private String cls;
        /**
         * The job id. Used to redirect users to the
         * job page on click.
         */
        private String jobId;
        /**
         * The job description.
         */
        private String description;

        /**
         * Default constructor.
         */
        public Series() {
        }

        /**
         * Constructor for series.
         *
         * @param times       the time points
         * @param label       the job name
         * @param jobId       the job id
         * @param description the job description
         */
        public Series(Point[] times, String label, Integer jobId, String description) {
            this.times = times;
            this.label = label;
            this.cls = label;
            this.jobId = jobId.toString();
            this.description = description;
        }
    }

    /**
     * Class job scheduler instance.
     */
    private final JobScheduler scheduler;

    /**
     * Constructor gets the job scheduler instance.
     */
    public JobTimeline() {
        scheduler = Store.getJobScheduler();
    }

    /**
     * Obtain the current timeline series data serialized as JSON.
     *
     * @param lookForwardGranularity the granularity of the look forward periods
     * @return timeline data as a JSON string
     * @throws IOException if an error querying the job queue occurs
     */
    public String getCurrentTimelineJson(Granularity lookForwardGranularity) throws IOException {
        Type serializedType = new TypeToken<Series[]>() { }.getType();
        return new Gson().toJson(getCurrentTimeline(lookForwardGranularity), serializedType);
    }

    /**
     * Get the series arrays that represent the job timeline data.
     *
     * @param lookForwardGranularity the granularity of the look forward periods
     * @return timeline data as a series array
     * @throws IOException if an error querying the job queue occurs
     */
    public Series[] getCurrentTimeline(Granularity lookForwardGranularity) throws IOException {
        int lookForwardPeriods = lookForwardGranularity.lookForwardPeriods();
        ZonedDateTime currentTime = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime lookForwardUntil = lookForwardGranularity.increment(currentTime, lookForwardPeriods);
        List<JobMetadata> scheduledJobs = scheduler.getAllQueue();
        return generateSeries(scheduledJobs, currentTime, lookForwardUntil);
    }

    /**
     * Generate the series data for a list of jobs between a start date and an end date.
     *
     * @param jobs      the list of jobs
     * @param startDate the start time of the timeline
     * @param endDate   the end time of the timeline
     * @return series array
     */
    public static Series[] generateSeries(List<JobMetadata> jobs, ZonedDateTime startDate, ZonedDateTime endDate) {
        long startTime = startDate.toInstant().toEpochMilli() / 1000L;
        long endTime = endDate.toInstant().toEpochMilli() / 1000L;
        Series[] seriesArray = new Series[jobs.size()];
        for (int i = 0; i < seriesArray.length; i++) {
            Point[] timelinePoints = generatePoints(jobs.get(i), startTime, endTime);
            JobMetadata job = jobs.get(i);
            Series timelineSeries = new Series(timelinePoints, job.getTestName(), job.getJobId(), job.getTestDescription());
            seriesArray[i] = timelineSeries;
        }
        return seriesArray;
    }

    /**
     * Generate the timeline points for a single jbo.
     *
     * @param job       the job
     * @param startTime the start time in seconds of the timeline
     * @param endTime   the end time in seconds of the timeline
     * @return the points for the job
     */
    public static Point[] generatePoints(JobMetadata job, long startTime, long endTime) {
        long jobEffectiveRunTime = job.getEffectiveRunTime() * 60L;
        Granularity jobGranularity = Granularity.getValue(job.getGranularity());
        long seconds = jobGranularity.getMinutes() * 60L;
        while (jobEffectiveRunTime < startTime) {
            jobEffectiveRunTime += seconds;
        }
        if (endTime <= jobEffectiveRunTime) {
            Point[] points = new Point[1];
            points[0] = new Point(endTime);
            points[0].display = "false";
            return points;
        } else {
            int deltaPeriods = (int) Math.ceil((endTime - jobEffectiveRunTime) / (double) seconds);
            if (deltaPeriods <= 0) {
                Point[] points = new Point[1];
                points[0] = new Point(endTime);
                points[0].display = "false";
                return points;
            }
            Point[] points = new Point[deltaPeriods];
            for (int i = 0; i < deltaPeriods; i++) {
                points[i] = new Point(jobEffectiveRunTime);
                jobEffectiveRunTime += seconds;
            }
            return points;
        }
    }
}
