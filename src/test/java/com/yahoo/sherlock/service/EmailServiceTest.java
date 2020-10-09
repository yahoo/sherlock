/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.service;

import com.beust.jcommander.internal.Lists;
import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.EmailMetaData;
import com.yahoo.sherlock.model.JobMetadata;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;
import com.yahoo.sherlock.store.AnomalyReportAccessor;
import com.yahoo.sherlock.store.DBTestHelper;
import com.yahoo.sherlock.store.EmailMetadataAccessor;
import com.yahoo.sherlock.utils.TimeUtils;

import org.mockito.Mockito;
import org.simplejavamail.email.Email;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

/**
 * Test for email service.
 */
public class EmailServiceTest {

    private AnomalyReportAccessor ara;
    private EmailMetadataAccessor ema;
    private static boolean status = true;
    private static EmailService emailService;
    private List<String> emails = Arrays.asList("owner@xyzmail.com");
    private static class MockEmailService extends EmailService {
        @Override
        protected boolean sendFormattedEmail(Email emailHandle) {
            return status;
        }

        @Override
        public Email createEmailHandle(String owner, List<String> ownerEmailIds) {
            return new Email();
        }
    }

    private static void inject(EmailService ems, String n, Object v) {
        try {
            Field f = EmailService.class.getDeclaredField(n);
            f.setAccessible(true);
            f.set(ems, v);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail(e.toString());
        }
    }

    @BeforeMethod
    public void setUp() throws Exception {
        emailService = new MockEmailService();
        ara = mock(AnomalyReportAccessor.class);
        ema = mock(EmailMetadataAccessor.class);
        inject(emailService, "anomalyReportAccessor", ara);
        inject(emailService, "emailMetadataAccessor", ema);
    }

    @Test
    public void testSendEmail() throws Exception {
        AnomalyReport anomalyReport = DBTestHelper.getNewReport();
        anomalyReport.setStatus(Constants.WARNING);
        Assert.assertTrue(emailService.sendEmail("owner", emails, Collections.singletonList(anomalyReport)));
        anomalyReport.setStatus(Constants.ERROR);
        Assert.assertTrue(emailService.sendEmail("owner", emails, Collections.singletonList(anomalyReport)));
        anomalyReport.setStatus(Constants.NODATA);
        Assert.assertTrue(emailService.sendEmail("owner", emails, Collections.singletonList(anomalyReport)));
        anomalyReport.setStatus(Constants.WARNING);
        status = false;
        Assert.assertFalse(emailService.sendEmail("owner", emails, Collections.singletonList(anomalyReport)));
    }
    @Test
    public void testException() throws Exception {
        Assert.assertFalse(emailService.sendEmail("owner", emails, null));
    }

    @Test
    public void testSendFormattedEmail() {
        emailService = new EmailService();
        Assert.assertFalse(emailService.sendFormattedEmail(new Email()));
    }

    @Test
    public void testValidateEmailDomain() {
        List<String> validDomains = Lists.newArrayList("yahoo", "hotmail");
        String[] valid = {"person1@yahoo.com", "person2@yahoo.cal", "person9.lastname1@hotmail.com"};
        String[] invalid = {"notanemail", "incomplete@mail", "tumblr.com", "johnny17@tumblr.com"};
        EmailService es = new EmailService();
        for (String s : valid) {
            Assert.assertTrue(es.validateEmail(s, validDomains));
        }
        for (String s : invalid) {
            Assert.assertFalse(es.validateEmail(s, validDomains));
        }
    }

    @Test
    public void testSendEmailOtherMethod() {
        JobMetadata jobMetadata = DBTestHelper.getNewJob();
        AnomalyReport anomalyReport = DBTestHelper.getNewReport();
        CLISettings.ENABLE_EMAIL = true;
        EmailService emailService = mock(EmailService.class);
        doCallRealMethod().when(emailService).processEmailReports(any(JobMetadata.class), anyList(), anyListOf(AnomalyReport.class));
        when(emailService.sendEmail(anyString(), anyList(), anyListOf(AnomalyReport.class))).thenReturn(true);
        // test for error report send
        anomalyReport.setStatus(Constants.ERROR);
        emailService.processEmailReports(jobMetadata, emails, Arrays.asList(anomalyReport));
        // test for error report not send
        when(emailService.sendEmail(anyString(), anyList(), anyListOf(AnomalyReport.class))).thenReturn(false);
        emailService.processEmailReports(jobMetadata, emails, Arrays.asList(anomalyReport));
        // test for nodata report not send
        anomalyReport.setStatus(Constants.NODATA);
        emailService.processEmailReports(jobMetadata, emails, Arrays.asList(anomalyReport));
        // test anomaly report send out
        AnomalyReport anomalyReport1 = DBTestHelper.getNewReport();
        anomalyReport.setStatus(Constants.WARNING);
        anomalyReport1.setStatus(Constants.NODATA);
        emailService.processEmailReports(jobMetadata, emails, Arrays.asList(anomalyReport, anomalyReport1));
        CLISettings.ENABLE_EMAIL = false;
    }

    private EmailMetaData getEmailMetadata(String email) {
        return new EmailMetaData(email);
    }

    @Test
    public void testSendConsolidatedEmail() throws IOException {
        EmailMetaData e1 = getEmailMetadata("abc@email.com");
        EmailMetaData e2 = getEmailMetadata("xyz@email.com");
        e2.setSendOutMinute("23");
        AnomalyReport ar = DBTestHelper.getNewReport();
        ar.setStatus(Constants.WARNING);
        // test for hourly trigger
        long time = 33 * 24 * 60L + 13 * 60L + 23;
        ZonedDateTime zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        when(ema.getAllEmailMetadataByTrigger(anyString())).thenReturn(Arrays.asList(e1, e2));
        when(ara.getAnomalyReportsForEmailId(anyString())).thenReturn(Arrays.asList(ar));
        emailService.sendConsolidatedEmail(zonedTime, "hour");
        Mockito.verify(ara, Mockito.times(1)).getAnomalyReportsForEmailId(e2.getEmailId());
        // test for daily/weekly/monthly trigger
        e2.setSendOutHour("13");
        emailService.sendConsolidatedEmail(zonedTime, "day");
        Mockito.verify(ara, Mockito.times(2)).getAnomalyReportsForEmailId(e2.getEmailId());
    }

    @Test
    public void testCreateEmailHandle() {
        List<String> emails = new ArrayList<>();
        emails.add("myemail@gmail.com");
        emails.add("email@yahoo.com");
        EmailService emailService = new EmailService();
        CLISettings.FROM_MAIL = "bla@bla.com";
        CLISettings.REPLY_TO = "xyz@xyz.com";
        Email email = emailService.createEmailHandle("aa", emails);
        CLISettings.FROM_MAIL = "bla@bla.com";
        Assert.assertEquals(email.getRecipients().size() , 2);
    }
}
