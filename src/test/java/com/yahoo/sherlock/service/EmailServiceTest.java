/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.beust.jcommander.internal.Lists;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.DBTestHelper;

import org.simplejavamail.email.Email;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test for email service.
 */
public class EmailServiceTest {

    private static boolean status = true;
    private static EmailService emailService;

    private static class MockEmailService extends EmailService {
        @Override
        protected boolean sendFormattedEmail(Email emailHandle) {
            return status;
        }

        @Override
        public Email createEmailHandle(String owner, String ownerEmailId) {
            return new Email();
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        emailService = new MockEmailService();
    }

    @Test
    public void testSendEmail() throws Exception {
        AnomalyReport anomalyReport = DBTestHelper.getNewReport();
        anomalyReport.setStatus(Constants.WARNING);
        Assert.assertTrue(emailService.sendEmail("owner", "owner@xyzmail.com", Collections.singletonList(anomalyReport)));
        anomalyReport.setStatus(Constants.ERROR);
        Assert.assertTrue(emailService.sendEmail("owner", "owner@xyzmail.com", Collections.singletonList(anomalyReport)));
        anomalyReport.setStatus(Constants.WARNING);
        status = false;
        Assert.assertFalse(emailService.sendEmail("owner", "owner@xyzmail.com", Collections.singletonList(anomalyReport)));
    }
    @Test
    public void testException() throws Exception {
        Assert.assertFalse(emailService.sendEmail("owner", "owner@xyzmail.com", null));
    }

    @Test
    public void testSendFormattedEmail() {
        emailService = new EmailService();
        Assert.assertFalse(emailService.sendFormattedEmail(new Email()));
    }

    @Test
    public void testValidateEmailDomain() {
        List<String> validDomains = Lists.newArrayList("yahoo", "hotmail");
        String[] valid = {"person1@yahoo.com", "person2@yahoo.ca", "person9.lastname1@edu.hotmail.com"};
        String[] invalid = {"notanemail", "incomplete@mail", "tumblr.com", "johnny17@tumblr.com"};
        EmailService es = new EmailService();
        for (String s : valid) {
            Assert.assertTrue(es.validateEmail(s, validDomains));
        }
        for (String s : invalid) {
            Assert.assertFalse(es.validateEmail(s, validDomains));
        }
    }

}
