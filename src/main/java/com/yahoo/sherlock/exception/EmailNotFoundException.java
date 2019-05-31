package com.yahoo.sherlock.exception;

/**
 * Exception thrown when no {@code EmailMetadata} can be
 * found with a specified emailId.
 */
public class EmailNotFoundException extends Exception {

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
        super(message);
    }

    /**
     * Constructor with a message and a throwable.
     *
     * @param message exception message
     * @param cause   cause of exception
     */
    public EmailNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

}
