/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.model.AnomalyReport;
import com.yahoo.sherlock.model.JsonTimeline;
import lombok.extern.slf4j.Slf4j;
import spark.QueryParamsMap;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * General utility class.
 */
@Slf4j
public class Utils {

    /**
     * Find all {@code Field}s in a given class with a specified {@code Annotation}.
     *
     * @param cls        the class to search
     * @param annotation the annotation for which to find fields
     * @return an array of fields with the specified annotation
     */
    public static Field[] findFields(Class<?> cls, Class<? extends Annotation> annotation) {
        Set<Field> fields = new HashSet<>();
        Class<?> c = cls;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(annotation)) {
                    fields.add(field);
                }
            }
            c = c.getSuperclass();
        }
        Field[] result = new Field[fields.size()];
        return fields.toArray(result);
    }

    /**
     * Convert a list of anomaly reports to a JSON timeline object,
     * where the timeline points have the timestamp as
     * {@link AnomalyReport#reportQueryEndTime report generation time}
     * and the type is the status of each anomaly report.
     *
     * @param anomalyReports the list of anomaly reports to convert
     * @return a JSON timeline of the reports
     */
    public static JsonTimeline getAnomalyReportsAsTimeline(List<AnomalyReport> anomalyReports) {
        Set<JsonTimeline.TimelinePoint> timelinePoints = new HashSet<>();
        for (AnomalyReport report : anomalyReports) {
            JsonTimeline.TimelinePoint timelinePoint = new JsonTimeline.TimelinePoint();
            if (report.getReportQueryEndTime() == null || report.getStatus() == null) {
                log.info("Empty report encountered!");
                continue;
            } else {
                timelinePoint.setType(report.getStatus());
                Integer timestamp = report.getReportQueryEndTime();
                timelinePoint.setTimestamp(timestamp.toString());
            }
            timelinePoints.add(timelinePoint);
        }
        JsonTimeline timeline = new JsonTimeline();
        timeline.setTimelinePoints(new ArrayList<>(timelinePoints));
        return timeline;
    }

    /**
     * Remove all keys in a set from a map.
     *
     * @param map  map do to removal
     * @param keys set of keys to remove
     * @param <K>  key type
     * @param <V>  value type
     */
    public static <K, V> void mapRemoveAll(Map<K, V> map, Set<K> keys) {
        for (K key : keys) {
            map.remove(key);
        }
    }

    /**
     * Deserialize an object from a query parameter map
     * keyed on the object's declared fields.
     *
     * @param t      object to populate
     * @param params query parameters
     * @param <T>    object type
     * @return populated object
     */
    public static <T> T deserializeQueryParams(T t, QueryParamsMap params) {
        Class<?> cls = t.getClass();
        Field[] fields = cls.getDeclaredFields();
        Map<String, String[]> multiMap = params.toMap();
        for (Field field : fields) {
            field.setAccessible(true);
            String name = field.getName();
            Class<?> type = field.getType();
            try {
                if (multiMap.containsKey(name)) {
                    String value = multiMap.get(name)[0];
                    if (value != null) {
                        switch (type.getName()) {
                            case "java.lang.Integer":
                                field.set(t, Integer.valueOf(value));
                                break;
                            case "java.lang.Float":
                                field.set(t, Float.valueOf(value));
                                break;
                            case "java.lang.Boolean":
                                field.set(t, Boolean.valueOf(value));
                                break;
                            case "java.lang.Long":
                                field.set(t, Long.valueOf(value));
                                break;
                            case "java.lang.Double":
                                field.set(t, Double.valueOf(value));
                                break;
                            default:
                                field.set(t, value);
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                log.error("Failed to set field!", e);
            }
        }
        return t;
    }

    /**
     * Convert a Spark {@code QueryParamsMap} to a string to string map.
     * This method assumes that all query parameters are simple strings.
     *
     * @param params query parameters
     * @return a string map of the parameters
     */
    public static Map<String, String> queryParamsToStringMap(QueryParamsMap params) {
        Map<String, String> stringParams = new HashMap<>();
        Map<String, String[]> multiMap = params.toMap();
        for (String key : multiMap.keySet()) {
            stringParams.put(key, multiMap.get(key)[0]);
        }
        return stringParams;
    }

    /**
     * Method to get SecretProvider based on custom class specified by {@link com.yahoo.sherlock.settings.CLISettings#CUSTOM_SECRET_PROVIDER_CLASS}.
     * @param className class name of the custom secret provider
     * @return SecretProvider instance of given class
     **/
    public static SecretProvider createSecretProvider(String className) {
        log.info("Initializing SecretProvider for class {}", className);
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> ctor = clazz.getConstructor();
            return (SecretProvider) ctor.newInstance();
        } catch (ClassNotFoundException e) {
            log.error("class name {} is not found", className);
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            log.error("no constructor for class {} found", className);
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            log.error("the class {} cannot be instantiated, probably an abstract class or interface?", className);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            log.error("the class {} constructor cannot be accessed", className);
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            log.error("the class {} constructor threw an exception {}", className, e);
            throw new RuntimeException(e);
        }
    }

}
