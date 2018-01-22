/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class LambdaExceptionTest {

    @Test
    public void testConsumerWithExceptions() {
        LambdaException.ConsumerWithExceptions<String, RuntimeException> g = s -> {
            throw new RuntimeException("error");
        };
        try {
            g.accept("hello");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), "error");
            return;
        }
        fail("Expected exception to be thrown");
    }

    @Test
    public void testFunctionWithExceptions() {
        LambdaException.FunctionWithExceptions<String, String, RuntimeException> g = s -> {
            throw new RuntimeException("error");
        };
        try {
            g.apply("hello");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), "error");
            return;
        }
        fail("Expected exception to be thrown");
    }

    @Test
    public void testConsumerExceptionHandler() {
        LambdaException.ConsumerWithExceptions<String, RuntimeException> g = s -> {
            throw new RuntimeException("error");
        };
        try {
            LambdaException.consumerExceptionHandler(g).accept("hello");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), "error");
            return;
        }
        fail("Expected exception to be thrown");
    }

    @Test
    public void testFunctionalExceptionHandler() {
        LambdaException.FunctionWithExceptions<String, String, RuntimeException> g = s -> {
            throw new RuntimeException("error");
        };
        try {
            LambdaException.functionalExceptionHandler(g).apply("hello");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), "error");
            return;
        }
        fail("Expected exception to be thrown");
    }

}
