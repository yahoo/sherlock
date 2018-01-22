/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.settings.DatabaseConstants;
import lombok.extern.slf4j.Slf4j;
import org.simplejavamail.email.Email;
import org.simplejavamail.mailer.Mailer;
import org.simplejavamail.mailer.config.ServerConfig;
import org.simplejavamail.mailer.config.TransportStrategy;
import spark.ModelAndView;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

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
            "^[a-zA-Z0-9_.+-]+@(?:(?:[a-zA-Z0-9-]+\\.)?[a-zA-Z]+\\.)?(%s)\\.[a-z]{0,3}$";

    /**
     * Gets a list of valid domains, which may be empty, from the
     * CLISettings.
     * @return list of valid domains
     */
    public List<String> getValidDomainsFromSettings() {
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
    public boolean validateEmail(String email, List<String> validDomains) {
        if (email == null || email.isEmpty()) {
            return false;
        }
        if (validDomains.isEmpty()) {
            // Empty domains list means all domains are valid
            return true;
        }
        StringJoiner joiner = new StringJoiner("|");
        for (String validDomain : validDomains) {
            joiner.add(validDomain);
        }
        String domainsMatcher = joiner.toString();
        return email.matches(String.format(EMAIL_PATTERN, domainsMatcher));
    }

    /**
     * Create the emaile handle.
     * @param owner owner name
     * @param ownerEmailId owner email id
     * @return email handle
     */
    public Email createEmailHandle(String owner, String ownerEmailId) {
        Email emailHandle = new Email();
        emailHandle.setFromAddress(Constants.SHERLOCK, CLISettings.FROM_MAIL);
        emailHandle.setReplyToAddress(Constants.SHERLOCK, CLISettings.REPLY_TO);
        emailHandle.addNamedToRecipients(owner, ownerEmailId);
        return emailHandle;
    }

    /**
     * Method for email service.
     * @param owner owner name
     * @param ownerEmailId owner email id
     * @param report anomaly report as a list of anomalies
     * @return status of email: true for success, false for error
     */
    public boolean sendEmail(String owner, String ownerEmailId, List<AnomalyReport> report) {
        Email emailHandle;
        try {
            // setting up email parameters
            emailHandle = createEmailHandle(owner, ownerEmailId);

            // Setting up HTML thymeleaf email reply text
            Map<String, Object> params = new HashMap<>();
            params.put(DatabaseConstants.ANOMALIES, report);
            params.put(Constants.EMAIL_HTML, "true");
            ThymeleafTemplateEngine thymeleafTemplateEngine = new ThymeleafTemplateEngine();
            if (!report.get(0).getStatus().equals(Constants.ERROR)) {
                // render the email HTML
                String messageHtml = thymeleafTemplateEngine.render(new ModelAndView(params, "table"));
                log.info("Thymeleaf rendered sunccessfully.");
                emailHandle.setSubject("Sherlock: Anomaly report");
                emailHandle.setTextHTML(messageHtml);
                emailHandle.addHeader("X-Priority", 5);
            } else {
                // send error mail if job failed with error
                params.put(Constants.EMAIL_ERROR, "true");
                params.put(Constants.JOB_ID, report.get(0).getJobId());
                String messageHtml = thymeleafTemplateEngine.render(new ModelAndView(params, "table"));
                emailHandle.setSubject("Sherlock: Anomaly report ERROR");
                emailHandle.setTextHTML(messageHtml);
                emailHandle.addHeader("X-Priority", 5);
            }
        } catch (Exception e) {
            log.error("Exception processing email body!", e);
            return false;
        }
        // send email
        log.info("Sending email to " + owner + " on email id: " + ownerEmailId);
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
                new ServerConfig(CLISettings.SMTP_HOST, CLISettings.SMTP_PORT),
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
