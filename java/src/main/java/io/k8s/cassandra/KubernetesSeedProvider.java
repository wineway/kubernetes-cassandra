
/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.k8s.cassandra;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.util.Config;
import org.apache.cassandra.locator.SeedProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Self discovery {@link SeedProvider} that creates a list of Cassandra Seeds by
 * communicating with the Kubernetes API.
 * <p>
 * Various System Variable can be used to configure this provider:
 * <ul>
 * <li>CASSANDRA_SERVICE defaults to cassandra</li>
 * <li>POD_NAMESPACE defaults to 'default'</li>
 * <li>CASSANDRA_SERVICE_NUM_SEEDS defaults to 8 seeds</li>
 * </ul>
 */
public class KubernetesSeedProvider implements SeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(KubernetesSeedProvider.class);

    private List<InetAddress> getEndpoints(String namespace, String service, String seeds) {
        try {
            ApiClient client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);

            CoreV1Api api = new CoreV1Api();
            V1Endpoints v1Endpoints = api.readNamespacedEndpoints(service, namespace, null, null, false);
            return Objects.requireNonNull(v1Endpoints.getSubsets()).stream().flatMap(o -> Objects.requireNonNull(o.getAddresses()).stream().flatMap(i -> {
                String ip = i.getIp();
                try {
                    return Stream.of(InetAddress.getByName(ip));
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                    return Stream.empty();
                }
            })).collect(Collectors.toList());
        } catch (IOException | ApiException | NullPointerException e) {
            logger.error("can't find endpoints due to: ", e);
            String[] split = seeds.split(",");
            ArrayList<InetAddress> inetAddressArrayList = new ArrayList<>();
            for (String s : split) {
                try {
                    InetAddress inetAddress = InetAddress.getByName(s);
                    inetAddressArrayList.add(inetAddress);
                } catch (UnknownHostException ex) {
                    logger.error("can't resolve ip addr of seeds node, check env CASSANDRA_SEEDS", ex);
                    System.exit(1);
                }
            }
            return inetAddressArrayList;
        }
    }


    /**
     * Create new seed provider
     *
     * @param params
     */
    public KubernetesSeedProvider(Map<String, String> params) {
    }

    /**
     * Call Kubernetes API to collect a list of seed providers
     *
     * @return list of seed providers
     */
    public List<InetAddress> getSeeds() {
        String service = getEnvOrDefault("CASSANDRA_SERVICE", "cassandra");
        String namespace = getEnvOrDefault("POD_NAMESPACE", "default");

        String initialSeeds = getEnvOrDefault("CASSANDRA_SEEDS", "");

        if ("".equals(initialSeeds)) {
            initialSeeds = getEnvOrDefault("POD_IP", "");
        }

        return getEndpoints(namespace, service, initialSeeds);
    }

    private static String getEnvOrDefault(String var, String def) {
        String val = System.getenv(var);
        if (val == null) {
            val = def;
        }
        return val;
    }

}
