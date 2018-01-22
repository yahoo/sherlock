package com.yahoo.sherlock.query;

import com.yahoo.sherlock.settings.CLISettingsTest;
import com.yahoo.sherlock.utils.Utils;

import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

public class EgadsConfigTest {

    @Test
    public void testFilteringMethod() {
        String[] exParams = {"0.01", "0.1", "10", "8", "0.99", "0.97"};
        EgadsConfig.FilteringMethod[] fms = EgadsConfig.FilteringMethod.values();
        for (int i = 0; i < fms.length; i++) {
            assertEquals(QueryBuilderTest.getValue("param", fms[i]), exParams[i]);
        }
    }

    @Test
    public void testEgadsConfigValueSetters() {
        EgadsConfig c = new EgadsConfig();
        Field[] fields = Utils.findFields(EgadsConfig.class, EgadsConfig.EgadsParam.class);
        for (Field f : fields) {
            assertNull(QueryBuilderTest.getValue(f, c));
        }
        c = EgadsConfig.create().buildDefault();
        for (Field f : fields) {
            assertNotNull(QueryBuilderTest.getValue(f, c));
        }
        EgadsConfig.Builder b = EgadsConfig.create();
        c = (EgadsConfig) QueryBuilderTest.getValue("config", b);
        @SuppressWarnings("unchecked")
        Map<String, Field> fmap = (Map<String, Field>) QueryBuilderTest.getValue(
            CLISettingsTest.getField("fieldMap", EgadsConfig.Builder.class), null);
        assertEquals(fields.length, fmap.size());
        b.setParam("invalidParam", "value");
        b.setParam("MAX_ANOMALY_TIME_AGO", "10");
        assertEquals(c.getMaxAnomalyTimeAgo(), "10");
        b.maxAnomalyTimeAgo(100);
        b.maxAnomalyTimeAgo(-10);
        assertEquals(c.getMaxAnomalyTimeAgo(), "100");
        b.maxAnomalyTimeAgo("ab");
        assertEquals(c.getMaxAnomalyTimeAgo(), "100");
        b.maxAnomalyTimeAgo("123");
        assertEquals(c.getMaxAnomalyTimeAgo(), "123");

        b.setParam("DETECTION_WINDOW_START_TIME", "1515009128");
        assertEquals(c.getDetectionWindowStartTime(), "1515009128");
        b.detectionWindowStartTime(1515019128L);
        b.detectionWindowStartTime(-10L);
        assertEquals(c.getDetectionWindowStartTime(), "1515019128");
        b.detectionWindowStartTime("ab");
        assertEquals(c.getDetectionWindowStartTime(), "1515019128");
        b.detectionWindowStartTime("1523019128");
        assertEquals(c.getDetectionWindowStartTime(), "1523019128");

        b.aggregation(1000);
        b.aggregation(-1);
        b.aggregation("-123");
        assertEquals(c.getAggregation(), "-123");
        b.aggregation("123");
        b.aggregation("asdfasdf");
        assertEquals(c.getAggregation(), "123");
        b.timeShifts(123);
        b.timeShifts(-1);
        b.timeShifts("-123");
        assertEquals(c.getTimeShifts(), "123");
        b.timeShifts("111");
        assertEquals(c.getTimeShifts(), "111");
        b.baseWindows(10, 1);
        b.baseWindows(-10, 10);
        assertEquals(c.getBaseWindows(), "1,10");
        b.baseWindows("2,10");
        b.baseWindows("10");
        assertEquals(c.getBaseWindows(), "2,10");
        b.period(10);
        b.period(-1);
        b.period("-1");
        assertEquals(c.getPeriod(), "-1");
        b.period("11000");
        assertEquals(c.getPeriod(), "11000");
        b.fillMissing(false);
        b.fillMissing("asdfasdf");
        assertEquals(c.getFillMissing(), "0");
        b.fillMissing("1");
        assertEquals(c.getFillMissing(), "1");
        b.fillMissing("false");
        assertEquals(c.getFillMissing(), "0");
        b.numWeeks(1);
        b.numWeeks(-1);
        b.numWeeks("12");
        b.numWeeks("-12");
        assertEquals("12", c.getNumWeeks());
        b.numToDrop(10);
        b.numToDrop(-10);
        b.numToDrop("123");
        b.numToDrop("-123");
        assertEquals(c.getNumToDrop(), "123");
        b.dynamicParameters(true);
        b.dynamicParameters("1");
        b.dynamicParameters("false");
        b.dynamicParameters("TTrue");
        assertEquals(c.getDynamicParameters(), "0");
        b.autoAnomalyPercent(0.2);
        b.autoAnomalyPercent("213123ffvv");
        b.autoAnomalyPercent("0.23");
        assertEquals(c.getAutoSensitivityAnomalyPercent(), "0.230");
        b.autoStandardDeviation(3.4);
        b.autoStandardDeviation(-3);
        b.autoStandardDeviation("123vv");
        b.autoStandardDeviation("33.5");
        assertEquals("33.50", c.getAutoSensitivityStandardDeviation());
        b.preWindowSize(-1);
        b.preWindowSize(10);
        b.preWindowSize("abc");
        b.preWindowSize("123");
        assertEquals("123", c.getPreWindowSize());
        b.postWindowSize(-1);
        b.postWindowSize(10);
        b.postWindowSize("abc");
        b.postWindowSize("123");
        assertEquals("123", c.getPostWindowSize());
        b.confidence(-1);
        b.confidence(12.3);
        b.confidence("abc");
        b.confidence("12.3");
        b.confidence("0.5");
        assertEquals("0.50", c.getConfidence());
        b.windowSize(20);
        b.windowSize(-10);
        b.windowSize("10");
        b.windowSize("abc");
        assertEquals("10", c.getWindowSize());
        b.filteringMethod("invalid");
        b.filteringMethod(EgadsConfig.FilteringMethod.GAP_RATIO);
        b.recommendedFilteringParam();
        assertEquals("GAP_RATIO", c.getFilteringMethod());
        assertEquals("0.01", c.getFilteringParam());
        b.filteringParam(10);
        b.filteringParam(-1);
        b.filteringParam("sdfa");
        b.filteringParam("10");
        assertEquals("10.0", c.getFilteringParam());
        b.build();
        b = EgadsConfig.create().filteringMethod("GAP_RATIO");
        c = b.build();
        assertEquals(c.getFilteringParam(), "0.01");
        Properties p = c.asProperties();
        assertEquals(p.get("FILTERING_METHOD"), "GAP_RATIO");
    }

}
