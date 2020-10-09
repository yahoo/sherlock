/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.utils.TimeUtils;

import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.ZonedDateTime;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


/**
 * Test for {@code EmailSenderTask}.
 */
public class EmailSenderTaskTest {

    private EmailSenderTask emailSenderTask;
    private EmailService emailService;

    @BeforeMethod
    public void setUp() throws Exception {
        emailSenderTask = mock(EmailSenderTask.class);
        emailService = mock(EmailService.class);
    }

    @Test
    public void testRun() throws Exception {
        doNothing().when(emailSenderTask).runEmailSender(anyLong());
        doCallRealMethod().when(emailSenderTask).run();
        emailSenderTask.run();
        Mockito.verify(emailSenderTask, Mockito.times(1)).runEmailSender(anyLong());
    }

    @Test
    public void testRunEmailSender() throws IOException {
        emailSenderTask = new EmailSenderTask();
        TestUtilities.inject(emailSenderTask, "emailService", emailService);
        Mockito.doNothing().when(emailService).sendConsolidatedEmail(any(), any());
        long time = 33 * 24 * 60L + 13 * 60L + 12L;
        ZonedDateTime zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfMonth(), 3);
        assertEquals(zonedTime.getDayOfWeek().getValue(), 2);
        // test without first day of month/first day of week(monday)
        emailSenderTask.runEmailSender(time);
        Mockito.verify(emailService, Mockito.times(2)).sendConsolidatedEmail(any(), any());
        // test with first day of month
        time = time - 2 * 24 * 60L;
        zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfMonth(), 1);
        emailSenderTask.runEmailSender(time);
        Mockito.verify(emailService, Mockito.times(5)).sendConsolidatedEmail(any(), any());
        // test with first day of week
        time = time + 1 * 24 * 60L;
        zonedTime = TimeUtils.zonedDateTimeFromMinutes(time);
        assertEquals(zonedTime.getDayOfWeek().getValue(), 1);
        emailSenderTask.runEmailSender(time);
        Mockito.verify(emailService, Mockito.times(8)).sendConsolidatedEmail(any(), any());
    }

    @Test(expectedExceptions = IOException.class)
    public void testRunEmailSenderException() throws Exception {
        emailSenderTask = new EmailSenderTask();
        TestUtilities.inject(emailSenderTask, "emailService", emailService);
        doThrow(new IOException()).when(emailService).sendConsolidatedEmail(any(), anyString());
        emailSenderTask.runEmailSender(1560384000 / 60L);
    }

}
