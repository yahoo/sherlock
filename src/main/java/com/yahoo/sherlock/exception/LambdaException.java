/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.exception;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Exception class for exceptions inside lambda expressions.
 */
public class LambdaException {

    /**
     * Consumer Interface.
     *
     * @param <T> Type of object to be consumed by the consumer interface
     * @param <E> Type of exception thrown by the consumer interface
     */
    @FunctionalInterface
    public interface ConsumerWithExceptions<T, E extends Exception> {
        /**
         * Accept type T.
         * Throws E.
         *
         * @param t type T object
         * @throws E exception type
         */
        void accept(T t) throws E;
    }

    /**
     * Functional Interface.
     *
     * @param <T> Type of input object to the functional interface
     * @param <R> Type of output object of the funactional interface
     * @param <E> Type of exception thrown by the functional interface
     */
    @FunctionalInterface
    public interface FunctionWithExceptions<T, R, E extends Exception> {
        /**
         * Apply to type T and return R.
         * Throws E.
         *
         * @param t type T object
         * @return type R object
         * @throws E exception type
         */
        R apply(T t) throws E;
    }

    /**
     * Use for Consumer interface.
     *
     * @param consumer Consumer with T type input and E type exception
     * @param <T>      Type of object to be consumed by the consumer
     * @param <E>      Type of exception thrown by the consumer
     * @return void
     * @throws E exception type
     */
    public static <T, E extends Exception> Consumer<T> consumerExceptionHandler(ConsumerWithExceptions<T, E> consumer) throws E {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Exception exception) {
                throwActualException(exception);
            }
        };
    }

    /**
     * Use for Functional interface.
     *
     * @param function Function with T type input, R type output and E type exception
     * @param <T>      Type of input object to the function
     * @param <R>      Type of output object of the funaction
     * @param <E>      Type of exception thrown by the function
     * @return R type object
     * @throws E exception type
     */
    public static <T, R, E extends Exception> Function<T, R> functionalExceptionHandler(FunctionWithExceptions<T, R, E> function) throws E {
        return t -> {
            try {
                return function.apply(t);
            } catch (Exception exception) {
                throwActualException(exception);
                return null;
            }
        };
    }

    /**
     * Unchecked exception.
     *
     * @param exception exception thrown from consumer or functional interface
     * @param <E>       thrown exception type
     * @throws E exception type
     */
    @SuppressWarnings("unchecked")
    private static <E extends Exception> void throwActualException(Exception exception) throws E {
        throw (E) exception;
    }
}
