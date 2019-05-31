package com.yahoo.sherlock.model;

import com.yahoo.sherlock.enums.Triggers;
import com.yahoo.sherlock.store.Attribute;

import java.io.Serializable;

import lombok.Data;

/**
 * Data storer for email id metadata.
 */
@Data
public class EmailMetaData implements Serializable {

    /**
     * Serialization id for uniformity across platform.
     */
    private static final long serialVersionUID = 31L;

    /**
     * EmailId value.
     */
    @Attribute
    private String emailId;

    /**
     * Hours value(0-23) to send out email.
     */
    @Attribute
    private String sendOutHour = "12";

    /**
     * Minutes value(0-59) to send out email.
     */
    @Attribute
    private String sendOutMinute = "00";

    /**
     * Email sendout repeat interval.
     * It can have six values: {@link Triggers}
     */
    @Attribute
    private String repeatInterval = Triggers.INSTANT.toString();

    /**
     * Default constructor.
     */
    public EmailMetaData() {

    }

    /**
     * Constructor to intialize default object.
     * @param emailId emailId to add
     */
    public EmailMetaData(String emailId) {
        this.emailId = emailId;
    }

    /**
     * Constructor with params.
     * @param emailId        emailId to add
     * @param sendOutHour    send out hour value(0-23)
     * @param sendOutMinute  send out hour value(0-23)
     * @param repeatInterval email send out interval value: possible values {@link Triggers}
     */
    public EmailMetaData(String emailId, String sendOutHour, String sendOutMinute, String repeatInterval) {
        this.emailId = emailId;
        this.sendOutHour = sendOutHour;
        this.sendOutMinute = sendOutMinute;
        this.repeatInterval = repeatInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EmailMetaData)) {
            return false;
        }

        EmailMetaData that = (EmailMetaData) o;

        return emailId.equals(that.emailId);
    }

    @Override
    public int hashCode() {
        return emailId.hashCode();
    }
}
