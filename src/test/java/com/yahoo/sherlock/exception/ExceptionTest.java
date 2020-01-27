/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Test class for all exceptions.
 */
public class ExceptionTest {

    @Test
    public void testClusterNotFoundExceptionConstructors() {
        // Default constructor
        ClusterNotFoundException e = new ClusterNotFoundException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        // Construtor with message
        e = new ClusterNotFoundException("some_message");
        assertEquals(e.getMessage(), ClusterNotFoundException.EXEPTION_MSG);
        assertNull(e.getCause());
        // Constructor with message and cause
        Exception cause = new RuntimeException("fake_message");
        e = new ClusterNotFoundException("some_message", cause);
        assertEquals(e.getMessage(), ClusterNotFoundException.EXEPTION_MSG);
        assertNotNull(e.getCause());
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testDruidExceptionConstructors() {
        DruidException e = new DruidException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        e = new DruidException("some_message");
        assertEquals(e.getMessage(), "some_message");
        assertNull(e.getCause());
        Exception cause = new RuntimeException("fake_message");
        e = new DruidException("some_message", cause);
        assertEquals(e.getMessage(), "some_message");
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testJobNotFoundExceptionConstructors() {
        JobNotFoundException e = new JobNotFoundException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        e = new JobNotFoundException("some_message");
        assertEquals(e.getMessage(), JobNotFoundException.EXEPTION_MSG);
        assertNull(e.getCause());
        Exception cause = new RuntimeException("fake_message");
        e = new JobNotFoundException("some_message", cause);
        assertEquals(e.getMessage(), JobNotFoundException.EXEPTION_MSG);
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testEmailNotFoundExceptionConstructors() {
        EmailNotFoundException e = new EmailNotFoundException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        e = new EmailNotFoundException("some_message");
        assertEquals(e.getMessage(), EmailNotFoundException.EXEPTION_MSG);
        assertNull(e.getCause());
        Exception cause = new RuntimeException("fake_message");
        e = new EmailNotFoundException("some_message", cause);
        assertEquals(e.getMessage(), EmailNotFoundException.EXEPTION_MSG);
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testSherlockExceptionConstructors() {
        SherlockException e = new SherlockException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        e = new SherlockException("some_message");
        assertEquals(e.getMessage(), "some_message");
        assertNull(e.getCause());
        Exception cause = new RuntimeException("fake_message");
        e = new SherlockException("some_message", cause);
        assertEquals(e.getMessage(), "some_message");
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testStoreExceptionConstructors() {
        StoreException e = new StoreException();
        assertNull(e.getMessage());
        assertNull(e.getCause());
        e = new StoreException("fake_message");
        assertEquals(e.getMessage(), "fake_message");
        assertNull(e.getCause());
        Exception cause = new RuntimeException("fake_message");
        e = new StoreException("some_message", cause);
        assertEquals(e.getMessage(), "some_message");
        assertEquals(e.getCause().getMessage(), "fake_message");
    }

    @Test
    public void testSchedulerExceptionConstructor() {
        SchedulerException e = new SchedulerException("g", new Exception("h"));
        assertEquals(e.getMessage(), "g");
        assertEquals(e.getCause().getMessage(), "h");
    }

}
