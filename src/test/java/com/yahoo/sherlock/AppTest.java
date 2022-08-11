/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock;

import com.beust.jcommander.JCommander;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.CLISettingsTest;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AppTest {

    @Test
    public void testHelp() throws IOException, SherlockException {
        JCommander jc = new JCommander();
        CLISettings settings = mock(CLISettings.class);
        CLISettingsTest.setField(CLISettingsTest.getField("jCommander", App.class), jc);
        CLISettingsTest.setField(CLISettingsTest.getField("settings", App.class), settings);
        App.main(new String[] {"--help"});
        verify(settings, times(1)).loadFromConfig();
        verify(settings, times(1)).print();
    }

    /**
     * Unit tests the App starts with --version argument.
     * Validates the loaded Prophet-related parameters match with the default value.
     */
    @Test
    public void testApp() throws IOException, SherlockException {
        JCommander jc = new JCommander();
        CLISettings settings = mock(CLISettings.class);
        App a = mock(App.class);
        CLISettingsTest.setField(CLISettingsTest.getField("jCommander", App.class), jc);
        CLISettingsTest.setField(CLISettingsTest.getField("settings", App.class), settings);
        CLISettingsTest.setField(CLISettingsTest.getField("app", App.class), a);
        App.main(new String[] {"--version", "v5.6.7"});
        assertEquals(CLISettings.PROPHET_URL, "127.0.0.1:4080");
        assertEquals(CLISettings.PROPHET_TIMEOUT, 120000);
        verify(a, times(1)).run();
    }

    @Test
    public void testRun() throws Exception {
        App a = mock(App.class);
        Mockito.doThrow(new SherlockException()).when(a).startWebServer();
        Mockito.doCallRealMethod().when(a).run();
        try {
            a.run();
        } catch (SherlockException e) {
            return;
        }
        Assert.fail("Expected exception");
    }

    @Test
    public void testStartWebServer() throws SherlockException {
        App a = mock(App.class);
        CLISettings.DEBUG_MODE = true;
        Mockito.doCallRealMethod().when(a).startWebServer();
        Mockito.doCallRealMethod().when(a).run();
        try {
            a.run();
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

}
