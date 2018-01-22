/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.store;

import java.util.HashMap;
import java.util.Properties;

/**
 * This class is used to store configuration parameters
 * for accessors.
 */
public class StoreParams extends HashMap<String, String> {

    /**
     * Build a store parameter object from a properties object.
     * @param props properties to use for parameters
     * @return a store parameter object
     */
    public static StoreParams fromProperties(Properties props) {
        StoreParams params = new StoreParams();
        for (String name : props.stringPropertyNames()) {
            params.put(name, props.getProperty(name));
        }
        return params;
    }

}
