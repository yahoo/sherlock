package com.yahoo.sherlock.exception;

/**
 * Exception thrown when no {@code EmailMetadata} can be
 * found with a specified emailId.
 */
public class EmailNotFoundException extends Exception {

    /** Exception message. */
    public static final String EXEPTION_MSG = "Email is not available";

    /**
     * Default constructor.
     */
    public EmailNotFoundException() {
        super();
    }

    /**
     * Constructor with a message.
     *
     * @param message exception message
     */
    public EmailNotFoundException(String message) {
        super(EXEPTION_MSG);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public EmailNotFoundException(String message, Throwable cause) {
        super(EXEPTION_MSG, cause);
    }

}
