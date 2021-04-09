/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.store.Store;
import lombok.extern.slf4j.Slf4j;
import org.simplejavamail.email.Email;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.config.ServerConfig;
import org.simplejavamail.mailer.config.TransportStrategy;
import spark.ModelAndView;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Emailer class for email service.
 */
@Slf4j
public class EmailService {

    /**
     * Incomplete regex that matches emails of a
     * certain domain. These are inserted separated
     * by '|' using String.format.
     */
    private static final String EMAIL_PATTERN =
            "^[a-zA-Z0-9_.+-]+@(%s).[a-z]{0,3}$";
    /**
     * Valid email regex.
     */
    private static final String VALID_EMAIL =
            "^[a-zA-Z0-9_.+-]+@(([a-zA-Z0-9-]+)(\\.[a-zA-Z0-9-]+)?).[a-z]{0,3}$";

    /**
     * EmailMetadataAccessor object.
     */
    protected EmailMetadataAccessor emailMetadataAccessor;

    /**
     * AnomalyReportAccessor object.
     */
    protected AnomalyReportAccessor anomalyReportAccessor;

    /**
     * Constructor.
     */
    public EmailService() {
        emailMetadataAccessor = Store.getEmailMetadataAccessor();
        anomalyReportAccessor = Store.getAnomalyReportAccessor();
    }

    /**
     * Gets a list of valid domains, which may be empty, from the
     * CLISettings.
     * @return list of valid domains
     */
    public static List<String> getValidDomainsFromSettings() {
        if (CLISettings.VALID_DOMAINS == null || CLISettings.VALID_DOMAINS.isEmpty()) {
            return Collections.emptyList();
        }
        String[] domains = CLISettings.VALID_DOMAINS.split(",");
        List<String> validDomains = new ArrayList<>(domains.length);
        for (String domain : domains) {
            validDomains.add(domain.trim());
        }
        return validDomains;
    }

    /**
     * Perform email validation. Checks for email form
     * and that the domain matches the valid domains list.
     * @param email email to validate
     * @param validDomains list of valid domains
     * @return true if the email matches
     */
    public static boolean validateEmail(String email, List<String> validDomains) {
        String[] allEmails = email.replace(" ", "").split(Constants.COMMA_DELIMITER);
        if (email.isEmpty()) {
            return false;
        }
        if (validDomains.isEmpty()) {
            boolean result = true;
            for (String oneEmail : allEmails) {
                result = result && oneEmail.matches(VALID_EMAIL);
            }
            return result;
        }
        StringJoiner joiner = new StringJoiner(Constants.PIPE_DELIMITER);
        for (String validDomain : validDomains) {
            joiner.add(validDomain);
        }
        String domainsMatcher = joiner.toString();
        boolean result = true;
        for (String oneEmail : allEmails) {
            String s = String.format(EMAIL_PATTERN, domainsMatcher);
            result = result && oneEmail.matches(s);
        }
        return result;
    }

    /**
     * Create the emaile handle.
     * @param owner owner name
     * @param ownerEmailIdList owner email ids list
     * @return email handle
     */
    public Email createEmailHandle(String owner, List<String> ownerEmailIdList) {
        Email emailHandle = new Email();
        emailHandle.setFromAddress(Constants.SHERLOCK, CLISettings.FROM_MAIL);
        emailHandle.setReplyToAddress(Constants.SHERLOCK, CLISettings.REPLY_TO);
        String[] emails = ownerEmailIdList.stream().toArray(size -> new String[size]);
        emailHandle.addNamedToRecipients(owner, emails);
        return emailHandle;
    }

    /**
     * Sends the consolidated alerts over the specified trigger period.
     * @param zonedDateTime current date as ZonedDateTime object
     * @param trigger trigger name value
     * @throws IOException io exception
     */
    public void sendConsolidatedEmail(ZonedDateTime zonedDateTime, String trigger) throws IOException {
        List<EmailMetaData> emails = emailMetadataAccessor.getAllEmailMetadataByTrigger(trigger);
        log.info("Found {} emails for {} trigger index", emails.size(), trigger);
        for (EmailMetaData emailMetaData : emails) {
            int hour = Integer.valueOf(emailMetaData.getSendOutHour());
            int minute = Integer.valueOf(emailMetaData.getSendOutMinute());
            boolean isTimeToSend = trigger.equalsIgnoreCase(Triggers.HOUR.toString()) ?
                                   zonedDateTime.getMinute() == minute :
                                   zonedDateTime.getHour() == hour && zonedDateTime.getMinute() == minute;
            if (isTimeToSend) {
                List<AnomalyReport> anomalyReports = anomalyReportAccessor.getAnomalyReportsForEmailId(emailMetaData.getEmailId());
                List<AnomalyReport> filteredReports = anomalyReports.stream()
                    .filter(a -> !a.getStatus().equalsIgnoreCase(Constants.SUCCESS))
                    .collect(Collectors.toList());
                log.info("Sending {} anomaly reports to {}", filteredReports.size(), emailMetaData.getEmailId());
                if (filteredReports.size() > 0) {
                    if (!sendEmail(Constants.SHERLOCK, Arrays.asList(emailMetaData.getEmailId()), filteredReports)) {
                        log.error("Error while sending email: {}, trigger: {}!", emailMetaData.getEmailId(), trigger);
                    }
                }
            }
        }
    }

    /**
     * Method to send emails to given emailIds.
     * @param job            job for which the anomalies are found
     * @param emails         list of email ids
     * @param anomalyReports list of anomaly reports
     */
    public void processEmailReports(JobMetadata job, List<String> emails, List<AnomalyReport> anomalyReports) {
        anomalyReports = anomalyReports.stream()
            .filter(a -> !a.getStatus().equalsIgnoreCase(Constants.SUCCESS))
            .collect(Collectors.toList());
        if (CLISettings.ENABLE_EMAIL) {
            if (isErrorCase(anomalyReports)) {
                if (!sendEmail(CLISettings.FAILURE_EMAIL, Arrays.asList(CLISettings.FAILURE_EMAIL), anomalyReports)) {
                    log.error("Error while sending failure email!");
                }
            } else if (isNoDataCase(anomalyReports)) {
                if (job.getEmailOnNoData()) {
                    if (!sendEmail(job.getOwner(), emails, anomalyReports)) {
                        log.error("Error while sending Nodata email!");
                    }
                }
            } else {
                if (!sendEmail(job.getOwner(), emails, anomalyReports)) {
                    log.error("Error while sending anomaly report email!");
                }
            }
        }
    }

    /**
     * Helper to identify Error case in anomaly reports.
     * @param anomalyReports input list of anomaly reports
     * @return true if error case else false
     */
    private boolean isErrorCase(List<AnomalyReport> anomalyReports) {
        return anomalyReports.size() == 1 && anomalyReports.get(0).getStatus().equals(Constants.ERROR);
    }

    /**
     * Helper to identify Nodata case in anomaly reports.
     * @param anomalyReports input list of anomaly reports
     * @return true if no data case else false
     */
    private boolean isNoDataCase(List<AnomalyReport> anomalyReports) {
        return anomalyReports.size() == 1 && anomalyReports.get(0).getStatus().equals(Constants.NODATA);
    }

    /**
     * Method for email service.
     * @param owner owner name
     * @param emails owner email ids list
     * @param anomalyReports anomaly report as a list of anomalies
     * @return status of email: true for success, false for error
     */
    public boolean sendEmail(String owner, List<String> emails, List<AnomalyReport> anomalyReports) {
        Email emailHandle;
        try {
            if (!emails.isEmpty()) {
                // setting up email parameters
                emailHandle = createEmailHandle(owner, emails);

                // Setting up HTML thymeleaf email reply text
                Map<String, Object> params = new HashMap<>();
                params.put(DatabaseConstants.ANOMALIES, anomalyReports);
                params.put(Constants.EMAIL_HTML, "true");
                ThymeleafTemplateEngine thymeleafTemplateEngine = new ThymeleafTemplateEngine();
                if (anomalyReports.size() > 0) {
                    if (!(isNoDataCase(anomalyReports) || isErrorCase(anomalyReports))) {
                        // render the email HTML
                        String messageHtml = thymeleafTemplateEngine.render(new ModelAndView(params, "table"));
                        log.info("Thymeleaf rendered sunccessfully.");
                        emailHandle.setSubject("Sherlock: Anomaly report");
                        emailHandle.setTextHTML(messageHtml);
                        emailHandle.addHeader("X-Priority", 5);
                    } else {
                        // send error mail if job failed with error
                        params.put(Constants.EMAIL_ERROR, "true");
                        params.put(Constants.JOB_ID, anomalyReports.get(0).getJobId());
                        params.put(Constants.TITLE, anomalyReports.get(0).getTestName());
                        String messageHtml = thymeleafTemplateEngine.render(new ModelAndView(params, "table"));
                        emailHandle.setSubject("Sherlock: Anomaly report ERROR");
                        emailHandle.setTextHTML(messageHtml);
                        emailHandle.addHeader("X-Priority", 5);
                    }
                } else {
                    return true;
                }
            } else {
                return true;
            }
        } catch (Exception e) {
            log.error("Exception processing email body!", e);
            return false;
        }
        // send email
        log.info("Sending email to " + owner + " on email ids: " + emails);
        return sendFormattedEmail(emailHandle);
    }

    /**
     * Method to send final formatted email.
     * @param emailHandle email handle
     * @return true or false: success or failure to send email
     */
    protected boolean sendFormattedEmail(Email emailHandle) {
        try {
            new Mailer(
                new ServerConfig(CLISettings.SMTP_HOST, CLISettings.SMTP_PORT, CLISettings.SMTP_USER, CLISettings.SMTP_PASSWORD),
                TransportStrategy.SMTP_TLS
            ).sendMail(emailHandle);
            log.info("Email sent successfully!");
        } catch (Exception e) {
            log.error("Exception in sending email!", e);
            return false;
        }
        return true;
    }
}
