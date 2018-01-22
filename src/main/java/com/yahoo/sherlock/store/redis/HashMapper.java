/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.store.redis;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for hashing
 * objects into key-value pairs and creating
 * objects from key-value pairs.
 */
@Slf4j
public class HashMapper extends Mapper<String> {

    /**
     * Hash an object. This method takes all object fields
     * annotated with {@code Attribute} and returns a {@code Map}
     * with {@code (fieldName, fieldValue)} pairs.
     *
     * @param obj the object to hash
     * @return the key-value pairs
     */
    @Override
    protected Map<String, String> performMap(Object obj) {
        // Grab the field values
        Map<String, String> hash = new HashMap<>();
        for (Field field : getFields()) {
            try {
                Object data = field.get(obj);
                hash.put(field.getName(), data == null ? "" : data.toString());
            } catch (IllegalAccessException e) {
                // Inaccessible fields will not stop the method
                log.error("Cannot access field {}", field.getName());
            }
        }
        return hash;
    }

    /**
     * Unhash a set of key-value pairs into an object instance.
     *
     * @param cls  the class of the object
     * @param hash the set of key-value pairs
     * @param <C>  the object type
     * @return an object instance with set fields
     */
    @Override
    protected <C> C performUnmap(Class<C> cls, Map<String, String> hash) {
        C obj = construct(cls);
        for (Field field : getFields()) {
            // Get and set the field value
            String value = hash.get(field.getName());
            try {
                field.set(obj, wrapType(field, value).get());
            } catch (IllegalAccessException e) {
                // Inaccessible fields will not stop the method
                log.error("Could not set field {}, inaccessible", field.getName());
            }
        }
        return obj;
    }

}
