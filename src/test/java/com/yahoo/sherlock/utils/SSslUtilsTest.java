/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.TestUtilities;
import com.yahoo.sherlock.settings.CLISettings;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContextBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class SSslUtilsTest {

    private SSslUtils sSslUtils;

    @BeforeMethod
    public void setUp() {
        sSslUtils = mock(SSslUtils.class);
    }

    @Test
    public void testIsPrincipalPresent() {
        doCallRealMethod().when(sSslUtils).isPrincipalAndTypePresent(anyString(), anyString());
        assertTrue(sSslUtils.isPrincipalAndTypePresent("name", "key").test("file.name.key.pem"));
        assertFalse(sSslUtils.isPrincipalAndTypePresent("name", "key").test("file.key.pem"));
        assertFalse(sSslUtils.isPrincipalAndTypePresent("name", "cert").test("file.key.pem"));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateConnectionSocketFactoryWithValidationNullPointerExceptionTest() {
        doCallRealMethod().when(sSslUtils).createConnectionSocketFactoryWithValidation(any());
        sSslUtils.createConnectionSocketFactoryWithValidation(null);
    }

    @Test
    public void testCreateConnectionSocketFactoryWithValidation() {
        doCallRealMethod().when(sSslUtils).createConnectionSocketFactoryWithValidation(any());
        sSslUtils.createConnectionSocketFactoryWithValidation(SSslTestHelper.getTestSSslConfigs());
        verify(sSslUtils, times(1)).createSslfWithValidation(any(SSslConfigs.class));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateSslfWithValidationNullPointerExceptionTest() {
        doCallRealMethod().when(sSslUtils).createSslfWithValidation(any());
        sSslUtils.createSslfWithValidation(null);
    }

    @Test
    public void testCreateSslfWithValidationReturnNullTest() {
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        sSslConfigs.setTrustStorePath("/abc/z/c.pem");
        sSslConfigs.setKeyStorePath(null);
        assertNull(sSslUtils.createSslfWithValidation(sSslConfigs));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testCreateSslfWithValidationNullSSLContextTest() {
        doCallRealMethod().when(sSslUtils).createSslfWithValidation(any());
        sSslUtils.createSslfWithValidation(SSslTestHelper.getTestSSslConfigs());
        verify(sSslUtils, times(1)).getHostNameVerifier(any(SSslConfigs.class));
        verify(sSslUtils, times(1)).setKeyStoreContext(any(SSLContextBuilder.class), any(SSslConfigs.class));
        verify(sSslUtils, times(1)).setTrustStoreContext(any(SSLContextBuilder.class), any(SSslConfigs.class));
    }

    @Test
    public void testCreateSslfWithValidationReturnExistingSSLContextTest() throws NoSuchAlgorithmException {
        doCallRealMethod().when(sSslUtils).createSslfWithValidation(any());
        doCallRealMethod().when(sSslUtils).getKey(any(SSslConfigs.class));
        doCallRealMethod().when(sSslUtils).getHostNameVerifier(any(SSslConfigs.class));
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        Map<String, SSLContext> sslDummyCache = new ConcurrentHashMap<>();
        TestUtilities.injectStaticFinal(sSslUtils, SSslUtils.class, "SSL_CONTEXT_MAP", sslDummyCache);
        SSLContext dummyContext = SSLContext.getDefault();
        sslDummyCache.put(sSslUtils.getKey(sSslConfigs), dummyContext);
        sSslUtils.createSslfWithValidation(sSslConfigs);
        verify(sSslUtils, times(1)).getHostNameVerifier(any(SSslConfigs.class));
        verify(sSslUtils, times(0)).setKeyStoreContext(any(SSLContextBuilder.class), any(SSslConfigs.class));
        verify(sSslUtils, times(0)).setTrustStoreContext(any(SSLContextBuilder.class), any(SSslConfigs.class));
    }

    @Test
    public void testGetHostNameVerifier() {
        doCallRealMethod().when(sSslUtils).getHostNameVerifier(any(SSslConfigs.class));
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        HostnameVerifier hostnameVerifier = sSslUtils.getHostNameVerifier(sSslConfigs);
        assertTrue(sSslConfigs.getStrict());
        assertTrue(hostnameVerifier instanceof DefaultHostnameVerifier);
        sSslConfigs.setStrict(false);
        hostnameVerifier = sSslUtils.getHostNameVerifier(sSslConfigs);
        assertFalse(sSslConfigs.getStrict());
        assertTrue(hostnameVerifier instanceof NoopHostnameVerifier);
    }

    @Test
    public void testGetKey() {
        doCallRealMethod().when(sSslUtils).getKey(any(SSslConfigs.class));
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        assertEquals(sSslUtils.getKey(sSslConfigs), sSslConfigs.getTrustStorePath() + sSslConfigs.getKeyStorePath());
    }

    @Test
    public void testCreateConnectionSocketFactoryWithoutValidation() {
        doCallRealMethod().when(sSslUtils).createConnectionSocketFactoryWithoutValidation();
        doCallRealMethod().when(sSslUtils).createSslfWithoutValidation();
        assertNotNull(sSslUtils.createConnectionSocketFactoryWithoutValidation());
    }

    @Test
    public void testCreateConnectionSocketFactoryWithCustomImpl() {
        doCallRealMethod().when(sSslUtils).createConnectionSocketFactoryWithCustomImpl(any(), anyString());
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        String customContextClassName = "com.yahoo.sherlock.utils.SpecificContext";
        assertNull(sSslUtils.createConnectionSocketFactoryWithCustomImpl(sSslConfigs, customContextClassName));
    }

    @Test
    public void testBuildSSLConfigs() {
        SSslConfigs sSslConfigs = SSslTestHelper.getTestSSslConfigs();
        sSslConfigs.setCertPath(null);
        sSslConfigs.setKeyPath(null);
        CLISettings.TRUSTSTORE_PATH = sSslConfigs.getTrustStorePath();
        CLISettings.KEYSTORE_PATH = sSslConfigs.getKeyStorePath();
        CLISettings.TRUSTSTORE_PASSWORD = sSslConfigs.getTrustStorePass();
        CLISettings.KEYSTORE_PASSWORD = sSslConfigs.getKeyStorePass();
        CLISettings.TRUSTSTORE_TYPE = sSslConfigs.getTrustStoreType();
        CLISettings.KEYSTORE_TYPE = sSslConfigs.getKeyStoreType();
        CLISettings.HTTPS_HOSTNAME_VERIFICATION = sSslConfigs.getStrict();

        doCallRealMethod().when(sSslUtils).buildSSLConfigs(anyString());
        doCallRealMethod().when(sSslUtils).getFilePath(anyString(), anyString(), anyString());
        doCallRealMethod().when(sSslUtils).isPrincipalAndTypePresent(anyString(), anyString());
        assertEquals(sSslUtils.buildSSLConfigs(null), sSslConfigs);
        assertEquals(sSslUtils.buildSSLConfigs(""), sSslConfigs);
        CLISettings.KEY_DIR = "src/test/resources";
        CLISettings.CERT_DIR = "src/test/resources";
        assertEquals(sSslUtils.buildSSLConfigs("file123"), sSslConfigs);
        sSslConfigs = new SSslConfigs();
        sSslConfigs.setCertPath("src/test/resources/file1.cert.empty");
        sSslConfigs.setKeyPath("src/test/resources/file1.key.empty");
        sSslConfigs.setStrict(CLISettings.HTTPS_HOSTNAME_VERIFICATION);
        assertEquals(sSslUtils.buildSSLConfigs("file1"), sSslConfigs);
    }
}
