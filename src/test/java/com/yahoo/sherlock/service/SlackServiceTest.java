package com.yahoo.sherlock.service;

import static org.testng.Assert.assertEquals;

import com.google.gson.GsonBuilder;
import com.slack.api.webhook.Payload;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JobMetadata;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SlackServiceTest {

    @Test
    public void testCreatePayload() throws IOException {
        SlackService slackService = new SlackService();
        JobMetadata job = new JobMetadata();
        AnomalyReport report = new AnomalyReport();
        report.setGroupByFilters("abc");
        report.setMetricName("metricName");
        report.setModelName("modelName");
        report.setModelParam("modelParam");
        report.setAnomalyTimestamps("337,554@554,557:560");
        report.setJobFrequency("hour");
        report.setStatus("goodo");
        report.setDeviationString("5");
        report.setJobId(12);
        report.setReportQueryEndTime(55512);
        Payload payload = slackService.createPayload(job, report);
        String payloadJson = new GsonBuilder().setPrettyPrinting().create().toJson(payload);
        String jsonString = new String(Files.readAllBytes(Paths.get("src/test/resources/slack_message.json")));
        assertEquals(jsonString, payloadJson);
    }
}