package com.yahoo.sherlock.service;

import com.slack.api.Slack;
import com.slack.api.model.Attachment;
import com.slack.api.model.Attachment.AttachmentBuilder;
import com.slack.api.model.Field;
import com.slack.api.webhook.Payload;
import com.slack.api.webhook.Payload.PayloadBuilder;
import com.slack.api.webhook.WebhookResponse;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.utils.NumberUtils;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SlackService {

    private static final Slack slack = Slack.getInstance();


    public void sendSlackMessage(JobMetadata job, List<AnomalyReport> reports) {
        reports.stream().filter(AnomalyReport::isHasAnomaly).forEach(report -> sendSlackMessage(job, report));
    }


    private void sendSlackMessage(final JobMetadata job, final AnomalyReport report) {
        try {
            log.info("Anomaly found - building Slack payload");
            Payload payload = createPayload(job, report);
            log.info("Sending {} to slack webhook {}", payload, CLISettings.SLACK_WEBHOOK);
            WebhookResponse response = slack.send(CLISettings.SLACK_WEBHOOK, payload);
            log.info("Slack response: {}", response);
        }
        catch (IOException e) {
            log.error("IOException during slack send", e);
            e.printStackTrace();
        }
    }


    private String buildChartLink(final AnomalyReport report) {
        String detectionWindow = "5";
        String chartLink = String.format("%s/Chart/%s/%s/%s",
                                         CLISettings.HTTP_BASE_URI,
                                         report.getJobId(),
                                         report.getReportQueryEndTime(),
                                         detectionWindow
        );

        String groupByFilters = report.getGroupByFilters();
        if (groupByFilters != null && !groupByFilters.isEmpty()) {
            chartLink += "/" + report.getSeriesName();
        }
        return chartLink;
    }


    Payload createPayload(final JobMetadata job, final AnomalyReport report) {
        PayloadBuilder payloadBuilder = Payload.builder();
        payloadBuilder.attachments(createAttachments(report));
        payloadBuilder.channel(job.getSlackChannel());
        return payloadBuilder.build();
    }


    @NotNull
    private ArrayList<Attachment> createAttachments(final AnomalyReport report) {
        String chartLink = buildChartLink(report);
        String deviation = report.getSlackFormattedDeviation();
        Long longDeviation = NumberUtils.parseLong(deviation, -1L);
        String attachmentColour = (longDeviation < 0) ? "danger" : "good";

        AttachmentBuilder attachment = Attachment.builder()
                                                 .mrkdwnIn(listOf("text"))
                                                 .color(attachmentColour)
                                                 .title(report.getMetricInfo())
                                                 .titleLink(chartLink)
                                                 .authorName("Sherlock Anomaly Detector")
                                                 .title(report.getMetricInfo())
                                                 .fields(createFields(report, deviation));

        ArrayList<Attachment> attachments = new ArrayList<>();
        attachments.add(attachment.build());
        return attachments;
    }


    @NotNull
    private List<Field> createFields(final AnomalyReport report, final String deviation) {
        List<Field> fields = new ArrayList<>();
        fields.add(field("Dimensions", report.getGroupByFilters(), false));
        fields.add(field("Model Info", report.getModelInfo(), true));
        fields.add(field("Status", report.getStatus(), true));
        fields.add(field("Date", report.getFormattedAnomalyTimestamps(), true));
        fields.add(field("Deviation", "*" + deviation + "%*", true));
        return fields;
    }


    private Field field(final String title, final String value, final boolean valueShortEnough) {
        return Field.builder().title(title).value(value).valueShortEnough(valueShortEnough).build();
    }


    private static <T> List<T> listOf(T t) {
        ArrayList<T> list = new ArrayList<>();
        list.add(t);
        return list;
    }
}
