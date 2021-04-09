/*
 * Copyright 2017, Yahoo Holdings Inc.
 * Copyrights licensed under the GPL License.
 * See the accompanying LICENSE file for terms.
 */

package com.yahoo.sherlock.utils;

import com.yahoo.sherlock.settings.Constants;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;

import java.util.Properties;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * SSL configuration holder class.
 */
@Slf4j
@Data
public class SSslConfigs {

    /** Used to mark SSL config parameters. **/
    @Retention(RetentionPolicy.RUNTIME)
    public @interface SSLProperty {

        /**
         * Parameter Name.
         * @return name of the param
         **/
        String name();
    }

    /** Trust store path. **/
    @SSLProperty(name = Constants.TRUSTSTORE_PATH)
    private String trustStorePath;

    /** Trust store type. **/
    @SSLProperty(name = Constants.TRUSTSTORE_TYPE)
    private String trustStoreType;

    /** Trust store password. **/
    @SSLProperty(name = Constants.TRUSTSTORE_PASS)
    private transient String trustStorePass;

    /** Key store path. **/
    @SSLProperty(name = Constants.KEYSTORE_PATH)
    private String keyStorePath;

    /** Key store type. **/
    @SSLProperty(name = Constants.KEYSTORE_TYPE)
    private String keyStoreType;

    /** Key store paaaword. **/
    @SSLProperty(name = Constants.KEYSTORE_PASS)
    private transient String keyStorePass;

    /** Private key path. **/
    @SSLProperty(name = Constants.KEY_PATH)
    private String keyPath;

    /** Public key path. **/
    @SSLProperty(name = Constants.CERT_PATH)
    private String certPath;

    /** Strict hostname varification boolean param. **/
    @SSLProperty(name = Constants.HOSTNAME_STRICT_VERIFICATION)
    private Boolean strict = true;

    /**
     * Builder for {@link com.yahoo.sherlock.utils.SSslConfigs}.
     */
    public static final class SSslConfigsBuilder {

        /** SSslConfigs. **/
        private SSslConfigs sSslConfigs;

        /** Constructor. **/
        private SSslConfigsBuilder() {
            sSslConfigs = new SSslConfigs();
        }

        /**
         * Method to get new builder.
         * @return SSslConfigsBuilder
         * **/
        public static SSslConfigsBuilder getSSslConfigsBuilder() {
            return new SSslConfigsBuilder();
        }

        /**
         * Set truststore path.
         * @param trustStorePath truststore path
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder trustStorePath(String trustStorePath) {
            this.sSslConfigs.trustStorePath = trustStorePath;
            return this;
        }

        /**
         * Set truststore type.
         * @param trustStoreType type of the truststore
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder trustStoreType(String trustStoreType) {
            this.sSslConfigs.trustStoreType = trustStoreType;
            return this;
        }

        /**
         * Set truststore pass.
         * @param trustStorePass truststore password
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder trustStorePass(String trustStorePass) {
            this.sSslConfigs.trustStorePass = trustStorePass;
            return this;
        }

        /**
         * Set keystore path.
         * @param keyStorePath keystore path
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder keyStorePath(String keyStorePath) {
            this.sSslConfigs.keyStorePath = keyStorePath;
            return this;
        }

        /**
         * Set keystore type.
         * @param keyStoreType type of the keystore
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder keyStoreType(String keyStoreType) {
            this.sSslConfigs.keyStoreType = keyStoreType;
            return this;
        }

        /**
         * Set keystore pass.
         * @param keyStorePass keystore password
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder keyStorePass(String keyStorePass) {
            this.sSslConfigs.keyStorePass = keyStorePass;
            return this;
        }

        /**
         * Set private key path.
         * @param keyPath path to key file
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder keyPath(String keyPath) {
            this.sSslConfigs.keyPath = keyPath;
            return this;
        }

        /**
         * Set public cert path.
         * @param certPath path to cert file
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder certPath(String certPath) {
            this.sSslConfigs.certPath = certPath;
            return this;
        }

        /**
         * Set/unset strict hostname verification.
         * @param isStrict enable/disable strict hostname verification
         * @return SSslConfigsBuilder
         **/
        public SSslConfigsBuilder isStrict(boolean isStrict) {
            this.sSslConfigs.strict = isStrict;
            return this;
        }

        /**
         * Build SSslConfigs.
         * @return SSslConfigs
         **/
        public SSslConfigs build() {
            return sSslConfigs;
        }
    }

    /**
     * Method to return SSL configs as {@link java.util.Properties}.
     * @return ssl properties
     */
    public Properties asProperties() {
        Field[] configFields = Utils.findFields(SSslConfigs.class, SSLProperty.class);
        Properties properties = new Properties();
        for (Field configField : configFields) {
            configField.setAccessible(true);
            String paramName = configField.getAnnotation(SSLProperty.class).name();
            try {
                properties.setProperty(paramName, String.valueOf(configField.get(this)));
            } catch (IllegalAccessException e) {
                log.error("Failed to convert field [{}] to properties!", paramName, e);
            }
        }
        return properties;
    }
}
