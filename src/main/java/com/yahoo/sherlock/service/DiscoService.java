package com.yahoo.sherlock.service;

import com.google.gson.JsonObject;
import com.yahoo.sherlock.exception.SherlockException;
import com.yahoo.sherlock.settings.CLISettings;


/**
 * Discovery service.
 */
public class DiscoService {
    /**
     * Discovers a service using it's name.
     * @param name the name of a service
     * @return Service object
     * @throws SherlockException
     */
    public Service getService(String name) throws SherlockException {
        final JsonObject json =
                new ServiceFactory().newHttpServiceInstance().getJson(CLISettings.DISCO_URL + name);
        return new Service(json.get("address").getAsString(), json.get("port").getAsInt());
    }

    /**
     * Basic service class representation.
     */
    public class Service {
        private String host;
        private Integer port;

        Service(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        /**
         * @return host field.
         */
        public String getHost() {
            return host;
        }

        /**
         * @return port field.
         */
        public Integer getPort() {
            return port;
        }
    }
}
