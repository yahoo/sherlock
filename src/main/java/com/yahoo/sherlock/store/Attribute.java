/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * {@code Data} class fields annotated with {@code Attribute}
 * will be stored in the backend.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Attribute {

    /**
     * Possible attribute types, identifying serialized object fields.
     */
    enum Type {
        UNSPECIFIED, INTEGER, LONG, DOUBLE, BOOLEAN, STRING
    }

    /**
     * The attribute type.
     * @return type
     */
    Type type() default Type.UNSPECIFIED;

}
