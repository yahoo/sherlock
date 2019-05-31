package com.yahoo.sherlock.model;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * EmailMetadata test class.
 */
public class EmailMetaDataTest {

    private EmailMetaData emailMetaData;

    @BeforeMethod
    public void setUp() throws Exception {
        emailMetaData = new EmailMetaData("email");
    }

    @Test
    public void testGetEmailId() throws Exception {
        assertEquals(emailMetaData.getEmailId(), "email");
    }

    @Test
    public void testGetSendOutHour() throws Exception {
        assertEquals(emailMetaData.getSendOutHour(), "12");
    }

    @Test
    public void testGetSendOutMinute() throws Exception {
        assertEquals(emailMetaData.getSendOutMinute(), "00");
    }

    @Test
    public void testGetRepeatInterval() throws Exception {
        assertEquals(emailMetaData.getRepeatInterval(), "instant");
    }

    @Test
    public void testSetEmailId() throws Exception {
        emailMetaData.setEmailId("emailll");
        assertEquals(emailMetaData.getEmailId(), "emailll");
    }

    @Test
    public void testSetSendOutHour() throws Exception {
        emailMetaData.setSendOutHour("23");
        assertEquals(emailMetaData.getSendOutHour(), "23");
    }

    @Test
    public void testSetSendOutMinute() throws Exception {
        emailMetaData.setSendOutMinute("54");
        assertEquals(emailMetaData.getSendOutMinute(), "54");
    }

    @Test
    public void testSetRepeatInterval() throws Exception {
        emailMetaData.setRepeatInterval("day");
        assertEquals(emailMetaData.getRepeatInterval(), "day");
    }
}
