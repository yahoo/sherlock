package com.yahoo.sherlock.scheduler;

import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.service.EmailService;
import com.yahoo.sherlock.utils.TimeUtils;

import java.io.IOException;
import java.time.ZonedDateTime;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Class for email sender runnable task.
 */
@NoArgsConstructor
@Slf4j
public class EmailSenderTask implements Runnable {

    /** Thread name prefix. */
    private static final String THREAD_NAME_PREFIX = "EmailSenderTask-";

    /**
     * Email Service obj to send emails.
     */
    private EmailService emailService = new EmailService();

    @Override
    public void run() {
        try {
            String name = THREAD_NAME_PREFIX + Thread.currentThread().getName();
            log.info("Running thread {}", name);
            runEmailSender(TimeUtils.getTimestampMinutes());
        } catch (IOException e) {
            log.error("Error while running email sender task!", e);
        }
    }

    /**
     * Method to send email if required at this time.
     * @param timestampMinutes input current timestamp in minutes
     * @throws IOException     if an error sending email
     */
    public void runEmailSender(long timestampMinutes) throws IOException {
        ZonedDateTime date = TimeUtils.zonedDateTimeFromMinutes(timestampMinutes);
        emailService.sendConsolidatedEmail(date, Triggers.DAY.toString());
        emailService.sendConsolidatedEmail(date, Triggers.HOUR.toString());
        if (date.getDayOfMonth() == 1) {
            emailService.sendConsolidatedEmail(date, Triggers.MONTH.toString());
        } else if (date.getDayOfWeek().getValue() == 1) {
            emailService.sendConsolidatedEmail(date, Triggers.WEEK.toString());
        }
    }
}
