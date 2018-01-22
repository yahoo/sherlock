package com.yahoo.sherlock;

import org.mockito.verification.VerificationMode;

import java.lang.reflect.Field;

import static org.mockito.Mockito.times;

public class TestUtilities {

    public static class TestException extends RuntimeException {
        public TestException(String message, Exception cause) {
            super(message, cause);
        }
    }

    public static VerificationMode ONCE = times(1);

    public static Field getField(Class<?> cls, String fieldName) {
        try {
            return cls.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    public static void inject(Object target, String fieldName, Object value) {
        Field field = getField(target.getClass(), fieldName);
        field.setAccessible(true);
        try {
            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    public static void inject(Object mock, Class<?> cls, String fieldName, Object value) {
        Field field = getField(cls, fieldName);
        field.setAccessible(true);
        try {
            field.set(mock, value);
        } catch (IllegalAccessException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    public static void inject(Class<?> target, String fieldName, Object value) {
        Field field = getField(target, fieldName);
        field.setAccessible(true);
        try {
            field.set(null, value);
        } catch (IllegalAccessException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    public static Object obtain(Object mock, Class<?> cls, String fieldName) {
        Field field = getField(cls, fieldName);
        field.setAccessible(true);
        try {
            return field.get(mock);
        } catch (IllegalAccessException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

    public static Object obtain(Object from, String fieldName) {
        Field field = getField(from.getClass(), fieldName);
        field.setAccessible(true);
        try {
            return field.get(from);
        } catch (IllegalAccessException e) {
            throw new TestException(e.getMessage(), e);
        }
    }

}
