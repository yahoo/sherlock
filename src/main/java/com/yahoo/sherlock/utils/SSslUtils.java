/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.service.SecretProviderService;
import com.yahoo.sherlock.settings.CLISettings;
import com.yahoo.sherlock.settings.Constants;

import org.apache.commons.lang.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.ssl.SSLContextBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * SSL utilities.
 */
@Slf4j
public class SSslUtils {

    /** Map to cache existing ssl context. **/
    private static final Map<String, SSLContext> SSL_CONTEXT_MAP = new ConcurrentHashMap<>();

    /** principal presence check. **/
    public Predicate<String> isPrincipalAndTypePresent(String principal, String type) {
        return s -> s.contains(principal) && s.contains(type);
    }

    /** Method to get SSLConnectionSocketFactory with validation. **/
    public SSLConnectionSocketFactory createConnectionSocketFactoryWithValidation(@NonNull SSslConfigs sSslConfigs) {
        log.info("Initializing SSLConnectionSocketFactory creation with validation");
        SSslConfigs.SSslConfigsBuilder sSslConfigsBuilder = SSslConfigs.SSslConfigsBuilder.getSSslConfigsBuilder()
            .trustStorePath(sSslConfigs.getTrustStorePath())
            .trustStorePass(sSslConfigs.getTrustStorePass())
            .trustStoreType(sSslConfigs.getTrustStoreType())
            .keyStorePath(sSslConfigs.getKeyStorePath())
            .keyStorePass(sSslConfigs.getKeyStorePass())
            .keyStoreType(sSslConfigs.getKeyStoreType());
        if (sSslConfigs.getStrict()) {
            log.info("Enabled hostname verification...");
            sSslConfigsBuilder.isStrict(sSslConfigs.getStrict());
        }
        return createSslfWithValidation(sSslConfigsBuilder.build());
    }

    /** Method to get SSLConnectionSocketFactory with validation. **/
    public SSLConnectionSocketFactory createSslfWithValidation(@NonNull SSslConfigs sSslConfigs) {
        if (!StringUtils.isNotEmpty(sSslConfigs.getTrustStorePath()) || !StringUtils.isNotEmpty(sSslConfigs.getKeyStorePath())) {
            log.error("Missing values for trustStorePath, keyStorePath parameters");
            return null;
        }
        String hashKey = getKey(sSslConfigs);
        SSLContext sslContext = SSL_CONTEXT_MAP.get(hashKey);
        if (sslContext != null) {
            log.info("Returned SSLContext from local cache");
            return new SSLConnectionSocketFactory(sslContext, getHostNameVerifier(sSslConfigs));
        }
        if (!StringUtils.isNotEmpty(sSslConfigs.getTrustStorePass()) || !StringUtils.isNotEmpty(sSslConfigs.getKeyStorePass())) {
            log.warn("Missing values for some or all of trustStorePass, keyStorePass parameters");
        }
        SSLContextBuilder builder = new SSLContextBuilder();
        try {
            synchronized (SSslUtils.class) {
                sslContext = SSL_CONTEXT_MAP.get(hashKey);
                if (sslContext != null) {
                    setKeyStoreContext(builder, sSslConfigs);
                    setTrustStoreContext(builder, sSslConfigs);
                    sslContext = builder.build();
                    SSL_CONTEXT_MAP.put(hashKey, sslContext);
                }
            }
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            String msg = "Unable to create sslContext with keyStorePath=" + sSslConfigs.getKeyStorePath() + ", trustStorePath=" + sSslConfigs.getTrustStorePath();
            log.error(msg, e);
            throw new SecurityException(msg, e);
        }
        return new SSLConnectionSocketFactory(Objects.requireNonNull(sslContext), getHostNameVerifier(sSslConfigs));
    }

    /** Method to load keystore. **/
    public void setKeyStoreContext(SSLContextBuilder builder, @NonNull SSslConfigs sSslConfigs) {
        if (StringUtils.isNotEmpty(sSslConfigs.getKeyStorePath()) && StringUtils.isNotEmpty(sSslConfigs.getKeyStorePass())) {
            log.info("Initializing sslContext with keyStorePath={}", sSslConfigs.getKeyStorePath());
            try (final InputStream in = new FileInputStream(new File(sSslConfigs.getKeyStorePath()))) {
                final KeyStore keystore = KeyStore.getInstance(sSslConfigs.getKeyStoreType());
                keystore.load(in, sSslConfigs.getKeyStorePass().toCharArray());
                builder.loadKeyMaterial(keystore, sSslConfigs.getKeyStorePass().toCharArray());
            } catch (IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException | KeyStoreException e) {
                String msg = "Exception in KeyStore setup with keyStorePath=" + sSslConfigs.getKeyStorePath();
                log.error(msg, e);
                throw new SecurityException(msg, e);
            }
            log.info("Initialized sslContext with keyStorePath={}", sSslConfigs.getKeyStorePath());
        } else {
            log.warn("Missing one/both of keyStorePath, keyStorePass");
        }
    }

    /** Method to load truststore. **/
    public void setTrustStoreContext(SSLContextBuilder builder, @NonNull SSslConfigs sSslConfigs) {
        if (StringUtils.isNotEmpty(sSslConfigs.getTrustStorePath())) {
            log.info("Initializing sslContext with trustStorePath={}", sSslConfigs.getTrustStorePath());
            try (final InputStream in = new FileInputStream(new File(sSslConfigs.getTrustStorePath()))) {
                final KeyStore trustStore = KeyStore.getInstance(sSslConfigs.getTrustStoreType());
                trustStore.load(in, sSslConfigs.getTrustStorePass() != null ? sSslConfigs.getTrustStorePass().toCharArray() : null);
                builder.loadTrustMaterial(trustStore, (TrustStrategy) (chain, authType) -> true);
            } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
                String msg = "Exception in TrustStore setup with trustStorePath=" + sSslConfigs.getTrustStorePath();
                log.error(msg, e);
                throw new SecurityException(msg, e);
            }
            log.info("Initialized sslContext with trustStorePath={}", sSslConfigs.getTrustStorePath());
        } else {
            log.warn("Missing trustStorePath");
        }
    }

    /** Method to get hostname verifier instance based on {@link SSslConfigs#getStrict()}. **/
    public HostnameVerifier getHostNameVerifier(@NonNull SSslConfigs sSslConfigs) {
        HostnameVerifier hostnameVerifier;
        if (sSslConfigs.getStrict()) {
            hostnameVerifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
            log.info("Using DefaultHostnameVerifier");
        } else {
            hostnameVerifier = NoopHostnameVerifier.INSTANCE;
            log.info("Using NoopHostnameVerifier");
        }
        return hostnameVerifier;
    }

    /** Method to get key for caching SSLContext into {@link com.yahoo.sherlock.utils.SSslUtils#SSL_CONTEXT_MAP}. **/
    public String getKey(@NonNull SSslConfigs sSslConfigs) {
        return sSslConfigs.getTrustStorePath() + sSslConfigs.getKeyStorePath();
    }

    /** Method to get SSLConnectionSocketFactory without validation. **/
    public SSLConnectionSocketFactory createConnectionSocketFactoryWithoutValidation() {
        return createSslfWithoutValidation();
    }

    /** Method to get SSLConnectionSocketFactory without validation. **/
    public SSLConnectionSocketFactory createSslfWithoutValidation() {
        log.info("Initializing SSLConnectionSocketFactory creation without validation");
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, (TrustStrategy) (chain, authType) -> true);
            return new SSLConnectionSocketFactory(builder.build(), new NoopHostnameVerifier());
        } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            String msg = "Unable to create SSLConnectionSocketFactory";
            log.error(msg, e);
            throw new SecurityException(msg, e);
        }
    }

    /** Method to get SSLConnectionSocketFactory based on custom class specified by {@link com.yahoo.sherlock.settings.CLISettings#CUSTOM_SSL_CONTEXT_PROVIDER_CLASS}. **/
    public SSLConnectionSocketFactory createConnectionSocketFactoryWithCustomImpl(@NonNull SSslConfigs sSslConfigs, String className) {
        log.info("Initializing custom SSLConnectionSocketFactory creation for class {}", className);
        try {
            Class<?> clazz = Class.forName(className);
            Constructor<?> ctor = clazz.getConstructor(Properties.class);
            SslContextProvider sslContextProvider = (SslContextProvider) ctor.newInstance(sSslConfigs.asProperties());
            return sslContextProvider.createConnectionSocketFactory();
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

    /** Method to get filename based on principal and type ("key" or "cert") if exist at given dir location. **/
    public Optional<String> getFilePath(String principal, String dir, String type) {
        try (Stream<Path> paths = Files.walk(Paths.get(dir))) {
            return paths.filter(Files::isRegularFile)
                .map(p -> p.getFileName().toString())
                .filter(isPrincipalAndTypePresent(principal, type))
                .findAny();
        } catch (IOException e) {
            log.error("IOException while searching for {} file for principal {} in dir {} : {}", type, principal, dir, e.getMessage());
        }
        return Optional.empty();
    }

    /** Method to build ssl configs from provided principal name. **/
    public SSslConfigs buildSSLConfigs(String principal) {
        if (principal != null && !principal.isEmpty()) {
            Optional<String> hasKeyFile = getFilePath(principal, CLISettings.KEY_DIR, Constants.KEY);
            Optional<String> hasCertFile = getFilePath(principal, CLISettings.CERT_DIR, Constants.CERT);
            if (hasKeyFile.isPresent() && hasCertFile.isPresent()) {
                return SSslConfigs.SSslConfigsBuilder.getSSslConfigsBuilder()
                    .keyPath(CLISettings.KEY_DIR + Constants.PATH_DELIMITER + hasKeyFile.get())
                    .certPath(CLISettings.CERT_DIR + Constants.PATH_DELIMITER + hasCertFile.get())
                    .isStrict(CLISettings.HTTPS_HOSTNAME_VERIFICATION)
                    .build();
            }
        }
        return SSslConfigs.SSslConfigsBuilder.getSSslConfigsBuilder()
            .trustStorePath(CLISettings.TRUSTSTORE_PATH)
            .trustStorePass(SecretProviderService.getKey(Constants.TRUSTSTORE_PASS))
            .trustStoreType(CLISettings.TRUSTSTORE_TYPE)
            .keyStorePath(CLISettings.KEYSTORE_PATH)
            .keyStorePass(SecretProviderService.getKey(Constants.KEYSTORE_PASS))
            .keyStoreType(CLISettings.KEYSTORE_TYPE)
            .isStrict(CLISettings.HTTPS_HOSTNAME_VERIFICATION)
            .build();
    }
}
