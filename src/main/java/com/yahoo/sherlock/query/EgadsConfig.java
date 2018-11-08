/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */
package com.yahoo.sherlock.query;

import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.utils.NumberUtils;
import com.yahoo.sherlock.utils.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class contains EGADS configuration parameters.
 */
@Slf4j
@Data
public class EgadsConfig {

    /**
     * Used to mark EGADS parameter fields.
     */
    @Retention(RetentionPolicy.RUNTIME)
    public @interface EgadsParam {
        /**
         * The name of the EGADS parameter.
         */
        String name();

        /**
         * The default value of the parameter.
         */
        String def();
    }

    /**
     * EGADS filtering method for time series.
     */
    public enum FilteringMethod {
        GAP_RATIO("0.01"),
        EIGEN_RATIO("0.1"),
        EXPLICIT("10"),
        K_GAP("8"),
        VARIANCE("0.99"),
        SMOOTHNESS("0.97");

        /**
         * Recommended filtering parameter value
         * for this filtering method.
         */
        private final String param;

        /**
         * Create a filtering method with the
         * recommended filtering parameter value.
         * @param param filtering parameter
         */
        FilteringMethod(String param) {
            this.param = param;
        }
    }

    /**
     * EGADS timeseries models.
     */
    public enum TimeSeriesModel {
        AutoForecastModel,
        DoubleExponentialSmoothingModel,
        MovingAverageModel,
        MultipleLinearRegressionModel,
        NaiveForecastingModel,
        OlympicModel,
        PolynomialRegressionModel,
        RegressionModel,
        SimpleExponentialSmoothingModel,
        TripleExponentialSmoothingModel,
        WeightedMovingAverageModel,
        SpectralSmoother;

        /**
         * get all the model names.
         * @return list of name of the models
         */
        public static List<String> getAllValues() {
            List<String> timeseriesModels = new ArrayList<>();
            for (TimeSeriesModel timeSeriesModel : TimeSeriesModel.values()) {
                timeseriesModels.add(timeSeriesModel.toString());
            }
            return timeseriesModels;
        }
    }

    /**
     * EGADS anomaly detection models.
     */
    public enum AnomalyDetectionModel {
        ExtremeLowDensityModel,
        AdaptiveKernelDensityChangePointDetector,
        KSigmaModel,
        NaiveModel,
        DBScanModel,
        SimpleThresholdModel;

        /**
         * get all the model names.
         * @return list of name of the models
         */
        public static List<String> getAllValues() {
            List<String> anomalyDetectionModels = new ArrayList<>();
            for (AnomalyDetectionModel anomalyDetectionModel : AnomalyDetectionModel.values()) {
                anomalyDetectionModels.add(anomalyDetectionModel.toString());
            }
            return anomalyDetectionModels;
        }
    }

    /**
     * Set all parameters of a config object to {@code null}.
     * @param emptyConfig the config object to set
     */
    private static void setAllNull(EgadsConfig emptyConfig) {
        Field[] configFields = Utils.findFields(EgadsConfig.class, EgadsParam.class);
        for (Field configField : configFields) {
            configField.setAccessible(true);
            try {
                configField.set(emptyConfig, null);
            } catch (IllegalAccessException e) {
                log.error("Failed to set default field [{}] to null!", configField.getName(), e);
            }
        }
    }

    /**
     * Set the parameters of a config object to their default
     * values. If {@code setAll} is enabled, all values are
     * set to their default regardless of the previous value,
     * otherwise only {@code null} values are set.
     * @param config the config object to set
     * @param setAll whether all fields should be set
     */
    private static void setToDefault(EgadsConfig config, boolean setAll) {
        Field[] configFields = Utils.findFields(EgadsConfig.class, EgadsParam.class);
        for (Field configField : configFields) {
            String defaultVal = configField.getAnnotation(EgadsParam.class).def();
            configField.setAccessible(true);
            try {
                if (configField.get(config) == null || setAll) {
                    configField.set(config, defaultVal);
                }
            } catch (IllegalAccessException e) {
                log.error("Could not set field [{}] to default value!", configField.getName(), e);
            }
        }
    }

    /**
     * Builder class for the EGADS config.
     */
    public static class Builder {
        /**
         * A map of EGADS parameter name to the field object.
         */
        private static Map<String, Field> fieldMap;

        // Initialize the field map
        static {
            Field[] configFields = Utils.findFields(EgadsConfig.class, EgadsParam.class);
            fieldMap = new HashMap<>((int) (configFields.length * 1.5));
            for (Field configField : configFields) {
                fieldMap.put(configField.getAnnotation(EgadsParam.class).name(), configField);
            }
        }

        /**
         * EgadsConfig instance.
         */
        private final EgadsConfig config;

        /**
         * Create a new EgadsConfig builder.
         * @param config config to use
         */
        private Builder(EgadsConfig config) {
            this.config = config;
        }

        /**
         * Set a specific EGADS parameter given a parameter name
         * and the string parameter value.
         * @param paramName the EGADS parameter name
         * @param paramValue the string parameter value
         * @return build instance
         */
        public Builder setParam(String paramName, String paramValue) {
            Field configField = fieldMap.get(paramName);
            if (configField == null) {
                return this;
            }
            configField.setAccessible(true);
            try {
                configField.set(config, paramValue);
            } catch (IllegalAccessException e) {
                log.error("Error while setting [{}] to [{}]", paramName, paramValue);
            }
            return this;
        }

        /**
         * @param val nonnegative integer number of hours
         *            for max anomaly time ago
         * @return builder instance
         */
        public Builder maxAnomalyTimeAgo(int val) {
            if (val < 0) {
                return this;
            }
            config.setMaxAnomalyTimeAgo(String.valueOf(val));
            return this;
        }

        /**
         * @param val max anomaly time ago string value
         * @return builder instance
         */
        public Builder maxAnomalyTimeAgo(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setMaxAnomalyTimeAgo(val);
            return this;
        }

        /**
         * @param val nonnegative integer
         * @return builder instance
         */
        public Builder detectionWindowStartTime(long val) {
            if (val < 0) {
                return this;
            }
            config.setDetectionWindowStartTime(String.valueOf(val));
            return this;
        }

        /**
         * @param val max anomaly time ago string value
         * @return builder instance
         */
        public Builder detectionWindowStartTime(String val) {
            if (!NumberUtils.isNonNegativeLong(val)) {
                return this;
            }
            config.setDetectionWindowStartTime(val);
            return this;
        }

        /**
         * @param val the amount of aggregation
         * @return builder instance
         */
        public Builder aggregation(int val) {
            if (val < 0) {
                return this;
            }
            config.setAggregation(String.valueOf(val));
            return this;
        }

        /**
         * @param val the amount of aggregation as a string
         * @return builder instance
         */
        public Builder aggregation(String val) {
            if (!NumberUtils.isInteger(val)) {
                return this;
            }
            config.setAggregation(val);
            return this;
        }

        /**
         * @param val nonnegative integer time shifts
         * @return builder instance
         */
        public Builder timeShifts(int val) {
            if (val < 0) {
                return this;
            }
            config.setTimeShifts(String.valueOf(val));
            return this;
        }

        /**
         * @param val time shifts string value
         * @return builder instance
         */
        public Builder timeShifts(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setTimeShifts(val);
            return this;
        }

        /**
         * @param first base window start
         * @param second base window end
         * @return builder instance
         */
        public Builder baseWindows(int first, int second) {
            if (first < 0 || second < 0) {
                return this;
            }
            if (first > second) {
                first = first + second;
                second = first - second;
                first = first - second;
            }
            config.setBaseWindows(String.format("%d,%d", first, second));
            return this;
        }

        /**
         * @param val base windows string value
         * @return builder instance
         */
        public Builder baseWindows(String val) {
            if (val == null || val.isEmpty()) {
                return this;
            }
            String[] vals = val.split(",");
            if (vals.length != 2 || !NumberUtils.isInteger(vals[0]) || !NumberUtils.isInteger(vals[1])) {
                return this;
            }
            return baseWindows(Integer.parseInt(vals[0]), Integer.parseInt(vals[1]));
        }

        /**
         * @param val nonegative integer value for period,
         *            and negative values will disable feature
         * @return builder instance
         */
        public Builder period(int val) {
            if (val < 0) {
                val = -1;
            }
            config.setPeriod(String.valueOf(val));
            return this;
        }

        /**
         * @param val period string value
         * @return builder instance
         */
        public Builder period(String val) {
            if (!NumberUtils.isInteger(val)) {
                return this;
            }
            return period(Integer.parseInt(val));
        }

        /**
         * @param val whether EGADS should fill missing values
         * @return builder instance
         */
        public Builder fillMissing(boolean val) {
            config.setFillMissing(val ? "1" : "0");
            return this;
        }

        /**
         * @param val fill missing string value
         * @return builder instance
         */
        public Builder fillMissing(String val) {
            if (NumberUtils.isInteger(val)) {
                return fillMissing(Integer.parseInt(val) > 0);
            } else if (NumberUtils.isBoolean(val)) {
                return fillMissing(Boolean.parseBoolean(val));
            }
            return this;
        }

        /**
         * @param val nonnegative integer number of weeks
         * @return builder instance
         */
        public Builder numWeeks(int val) {
            if (val < 0) {
                return this;
            }
            config.setNumWeeks(String.valueOf(val));
            return this;
        }

        /**
         * @param val number of weeks string value
         * @return builder instance
         */
        public Builder numWeeks(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setNumWeeks(val);
            return this;
        }

        /**
         * @param val nonnegative integer number of values to drop
         * @return builder instance
         */
        public Builder numToDrop(int val) {
            if (val < 0) {
                return this;
            }
            config.setNumToDrop(String.valueOf(val));
            return this;
        }

        /**
         * @param val number of values to drop string value
         * @return builder instance
         */
        public Builder numToDrop(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setNumToDrop(val);
            return this;
        }

        /**
         * @param val whether EGADS should use dynamic parameters
         * @return builder instance
         */
        public Builder dynamicParameters(boolean val) {
            config.setDynamicParameters(val ? "1" : "0");
            return this;
        }

        /**
         * @param val dynamic parameters string value
         * @return builder instance
         */
        public Builder dynamicParameters(String val) {
            if (NumberUtils.isInteger(val)) {
                return dynamicParameters(Integer.parseInt(val) > 0);
            } else if (NumberUtils.isBoolean(val)) {
                return dynamicParameters(Boolean.parseBoolean(val));
            }
            return this;
        }

        /**
         * @param val expected anomaly percent value between 0 and 1
         * @return builder instance
         */
        public Builder autoAnomalyPercent(double val) {
            if (val < 0 || val > 1) {
                return this;
            }
            config.setAutoSensitivityAnomalyPercent(String.format("%.3f", val));
            return this;
        }

        /**
         * @param val string expected anomaly percent value
         * @return builder instance
         */
        public Builder autoAnomalyPercent(String val) {
            if (!NumberUtils.isDouble(val)) {
                return this;
            }
            return autoAnomalyPercent(Double.parseDouble(val));
        }

        /**
         * @param val nonnegative standard deviation number
         * @return builder instance
         */
        public Builder autoStandardDeviation(double val) {
            if (val < 0) {
                return this;
            }
            config.setAutoSensitivityStandardDeviation(String.format("%.2f", val));
            return this;
        }

        /**
         * @param val standard deviation string value
         * @return builder instance
         */
        public Builder autoStandardDeviation(String val) {
            if (!NumberUtils.isDouble(val)) {
                return this;
            }
            return autoStandardDeviation(Double.parseDouble(val));
        }

        /**
         * @param val nonnegative integer pre window size value
         * @return builder instance
         */
        public Builder preWindowSize(int val) {
            if (val < 0) {
                return this;
            }
            config.setPreWindowSize(String.valueOf(val));
            return this;
        }

        /**
         * @param val pre window size string value
         * @return builder instance
         */
        public Builder preWindowSize(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setPreWindowSize(val);
            return this;
        }

        /**
         * @param val nonnegative integer post window size value
         * @return builder instance
         */
        public Builder postWindowSize(int val) {
            if (val < 0) {
                return this;
            }
            config.setPostWindowSize(String.valueOf(val));
            return this;
        }

        /**
         * @param val post window size string value
         * @return builder instance
         */
        public Builder postWindowSize(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setPostWindowSize(val);
            return this;
        }

        /**
         * @param val confidence double value between 0 and 1
         * @return builder instance
         */
        public Builder confidence(double val) {
            if (val < 0 || val > 1) {
                return this;
            }
            config.setConfidence(String.format("%.2f", val));
            return this;
        }

        /**
         * @param val confidence string value
         * @return builder instance
         */
        public Builder confidence(String val) {
            if (!NumberUtils.isDouble(val)) {
                return this;
            }
            return confidence(Double.parseDouble(val));
        }

        /**
         * @param val nonnegative integer window size value
         * @return builder instance
         */
        public Builder windowSize(int val) {
            if (val < 0) {
                return this;
            }
            config.setWindowSize(String.valueOf(val));
            return this;
        }

        /**
         * @param val window size string value
         * @return builder instance
         */
        public Builder windowSize(String val) {
            if (!NumberUtils.isNonNegativeInt(val)) {
                return this;
            }
            config.setWindowSize(val);
            return this;
        }

        /**
         * @param method filtering method to use
         * @return builder instance
         */
        public Builder filteringMethod(FilteringMethod method) {
            config.setFilteringMethod(method.toString());
            return this;
        }

        /**
         * @param value filtering method string name
         * @return builder instance
         */
        public Builder filteringMethod(String value) {
            if (value == null || value.isEmpty()) {
                return this;
            }
            try {
                return filteringMethod(FilteringMethod.valueOf(value.toUpperCase()));
            } catch (IllegalArgumentException e) {
                return this;
            }
        }

        /**
         * Set the filtering parameter value to the
         * recommended value based on the currently
         * selected filtering method. Sets the default
         * filtering method if none has been set.
         * @return builder instance
         */
        public Builder recommendedFilteringParam() {
            if (config.getFilteringMethod() == null) {
                config.setFilteringMethod(FilteringMethod.GAP_RATIO.toString());
            }
            config.setFilteringParam(FilteringMethod.valueOf(config.getFilteringMethod()).param);
            return this;
        }

        /**
         * @param val nonnegative filter param double value
         * @return builder instance
         */
        public Builder filteringParam(double val) {
            if (val < 0) {
                return this;
            }
            config.setFilteringParam(String.valueOf(val));
            return this;
        }

        /**
         * @param val string filtering parameter value
         * @return builder instance
         */
        public Builder filteringParam(String val) {
            if (!NumberUtils.isDouble(val)) {
                return this;
            }
            return filteringParam(Double.parseDouble(val));
        }

        /**
         * @return EGADS config with all fields set to default
         */
        public EgadsConfig buildDefault() {
            setToDefault(config, true);
            return config;
        }

        /**
         * Set remaining fields to default and build
         * EGADS config.
         * @return EGADS config
         */
        public EgadsConfig build() {
            if (config.getFilteringMethod() != null && config.getFilteringParam() == null) {
                this.recommendedFilteringParam();
            }
            setToDefault(config, false);
            return config;
        }
    }

    /**
     * @return a new EGADS config builder
     */
    public static Builder create() {
        return new Builder(new EgadsConfig());
    }

    /**
     * The maximum hours before the last time series
     * data point during which an anomaly will be
     * denoted. If set to 0, then only anomalies that
     * occur in the last time series data point will
     * be shown.
     */
    @EgadsParam(name = "MAX_ANOMALY_TIME_AGO", def = "0")
    private String maxAnomalyTimeAgo;

    /**
     * Alternative to "MAX_ANOMALY_TIME_AGO" parameter.
     * The Anomaly Detection Window starting time in senconds.
     * Start time is excluded from anomaly detection window,
     * If set to "0" then "MAX_ANOMALY_TIME_AGO" comes
     * into effect.
     */
    @EgadsParam(name = "DETECTION_WINDOW_START_TIME", def = "0")
    private String detectionWindowStartTime;

    /**
     * Denotes the level of aggregation for the
     * time series. If set to 1 or less, the setting
     * is ignored.
     */
    @EgadsParam(name = "AGGREGATION", def = "1")
    private String aggregation;

    /**
     * The EGADS operation type. For Sherlock
     * this should always be {@code "DETECT_ANOMALY"}.
     */
    @EgadsParam(name = "OP_TYPE", def = "DETECT_ANOMALY")
    private String opType;

    /**
     * The time series model type used to generate
     * the expected time series from which anomalies
     * are detected. As of now, this setting should
     * always be {@code "OlympicModel"} for Sherlock.
     */
    @EgadsParam(name = "TS_MODEL", def = "OlympicModel")
    private String tsModel;

    /**
     * The model used to compare the time series model
     * to the actual time series to detect anomalous points.
     * As of now, this setting should always be
     * {@code "KSigmaModel"} for Sherlock.
     */
    @EgadsParam(name = "AD_MODEL", def = "KSigmaModel")
    private String adModel;

    /**
     * EGADS input source. {@code ['CSV', 'STD_IN']}.
     */
    @EgadsParam(name = "INPUT", def = "CSV")
    private String input;

    /**
     * Where output should be directed. {@code ['STD_OUT', 'GUI']}.
     */
    @EgadsParam(name = "OUTPUT", def = "STD_OUT")
    private String output;

    /**
     * The number of possible time-shifts allowed
     * for the Olympic scoring model.
     */
    @EgadsParam(name = "TIME_SHIFTS", def = "0")
    private String timeShifts;

    /**
     * The first of the possible base windows for
     * Olympic scoring. Windows are between
     * {@code baseWindow[0] <= w <= baseWindow[1]}.
     */
    @EgadsParam(name = "BASE_WINDOWS", def = "1,7")
    private String baseWindows;

    /**
     * Specifies the periodicity of the time series.
     * Set to 0 for EGADS to auto detect periodicity,
     * set to -1 to disable the option.
     */
    @EgadsParam(name = "PERIOD", def = "0")
    private String period;

    /**
     * Whether egads should fill in missing values.
     * Set to 1 to enable.
     */
    @EgadsParam(name = "FILL_MISSING", def = "1")
    private String fillMissing;

    /**
     * The number of weeks that should be used
     * in Olympic scoring.
     */
    @EgadsParam(name = "NUM_WEEKS", def = "8")
    private String numWeeks;

    /**
     * The number of the highest and lowest points
     * to drop in the series.
     */
    @EgadsParam(name = "NUM_TO_DROP", def = "1")
    private String numToDrop;

    /**
     * If dynamic parameters is enable, set to 1,
     * then EGADS will automatically vary parameters
     * e.g. {@code numWeeks} to produce the best fit.
     */
    @EgadsParam(name = "DYNAMIC_PARAMETERS", def = "0")
    private String dynamicParameters;

    /**
     * The expected percentage of anomalies in the
     * time series data.
     */
    @EgadsParam(name = "AUTO_SENSITIVITY_ANOMALY_PCNT", def = "0.01")
    private String autoSensitivityAnomalyPercent;

    /**
     * The expected cluster standard deviation.
     */
    @EgadsParam(name = "AUTO_SENSITIVITY_SD", def = "3.0")
    private String autoSensitivityStandardDeviation;

    /**
     * The detection size before the main window.
     */
    @EgadsParam(name = "PRE_WINDOW_SIZE", def = "48")
    private String preWindowSize;
    /**
     * The detection size after the main window.
     */
    @EgadsParam(name = "POST_WINDOW_SIZE", def = "48")
    private String postWindowSize;
    /**
     * Confidence level at which divergence for
     * anomalies is computed.
     */
    @EgadsParam(name = "CONFIDENCE", def = "0.8")
    private String confidence;
    /**
     * Window size for spectral smoothing. Should be a
     * value larger than the size of the most important
     * seasonality.
     */
    @EgadsParam(name = "WINDOW_SIZE", def = "192")
    private String windowSize;

    /**
     * The filtering method to use for spectral smoothing.
     * {@code ['GAP_RATIO', 'EIGEN_RATIO', 'EXPLICIT', 'K_GAP', 'VARIANCE', 'SMOOTHNESS'}.
     */
    @EgadsParam(name = "FILTERING_METHOD", def = "GAP_RATIO")
    private String filteringMethod;
    /**
     * Filtering parameter for the filtering method.
     * Recommended are {@code [0.01, 0.1, 10, 8, 0.99, 0.97]}.
     */
    @EgadsParam(name = "FILTERING_PARAM", def = "0.01")
    private String filteringParam;

    /**
     * Create a new EGADS config will all fields
     * set to null.
     */
    public EgadsConfig() {
        setAllNull(this);
    }

    /**
     * Convert this EGADS config object to a properties
     * object using the parameter names.
     * @return EGADS properties
     */
    public Properties asProperties() {
        Field[] configFields = Utils.findFields(EgadsConfig.class, EgadsParam.class);
        Properties properties = new Properties();
        for (Field configField : configFields) {
            configField.setAccessible(true);
            String paramName = configField.getAnnotation(EgadsParam.class).name();
            try {
                if (configField.get(this) == null) {
                    properties.setProperty(paramName, configField.getAnnotation(EgadsParam.class).def());
                } else {
                    properties.setProperty(paramName, (String) configField.get(this));
                }
            } catch (IllegalAccessException e) {
                log.error("Failed to convert field [{}] to properties!", paramName, e);
            }
        }
        return properties;
    }

    /**
     * Set an egads config from a properties. This method
     * ignores property values that are invalid.
     *
     * @param properties properties object to create a config with
     * @return egads configuration
     */
    public static EgadsConfig fromProperties(Properties properties) {
        EgadsConfig config = new EgadsConfig();
        EgadsConfig.Builder builder = new EgadsConfig.Builder(config);
        for (String key : properties.stringPropertyNames()) {
            builder.setParam(key, properties.getProperty(key));
        }
        return config;
    }

    /**
     * Method to read configs from file.
     * @return configs properties object
     */
    public static Properties fromFile() {
        Properties p = null;
        try {
            // use the egads config file if available
            InputStream inputStream = new FileInputStream(CLISettings.EGADS_CONFIG_FILENAME);
            Properties properties = new Properties();
            properties.load(inputStream);
            if (!properties.isEmpty()) {
                p = properties;
            }
        } catch (Exception e) {
            log.error("Error, could not load EGADS configuration from file!", e);
        }
        return p;
    }
}
