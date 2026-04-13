/*
 * Copyright 2026 Red Hat Inc., and individual contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jgroups.protocols.azure;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.util.Util;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;

/**
 * Tests against a containerized Azurite service using both blob_storage_uri and connection_string.
 *
 * @author Radoslav Husar
 */
@RunWith(Parameterized.class)
public class AzuriteAZURE_PINGDiscoveryTestCase extends AbstractAZURE_PINGDiscoveryTestCase {

    public enum ConfigurationType {
        BLOB_STORAGE_URI,
        CONNECTION_STRING,
    }

    // These are fixed; well-known configuration properties for Azurite container
    private static final String AZURITE_ACCOUNT_NAME = "devstoreaccount1";
    private static final String AZURITE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final int AZURITE_BLOB_PORT = 10000;

    // All known property keys used in the tcp-azure.xml stack file
    private static final String[] PROPERTY_KEYS = {"azure.connection_string", "azure.blob_storage_uri", "azure.account_name", "azure.access_key", "azure.container"};

    private static GenericContainer<?> azurite;
    private static final Map<String, String> savedProperties = new HashMap<>();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<ConfigurationType> data() {
        return Arrays.asList(ConfigurationType.values());
    }

    private final ConfigurationType configurationType;

    public AzuriteAZURE_PINGDiscoveryTestCase(ConfigurationType configurationType) {
        this.configurationType = configurationType;
    }

    @BeforeClass
    public static void setUp() {
        if (!isDockerAvailable() && !Util.checkForLinux()) {
            Assume.assumeTrue("Podman/Docker environment is not available - skipping tests against Azurite service.", false);
        } else if (!isDockerAvailable()) {
            fail("Credentials are not provided, thus Podman/Docker on Linux is required to run tests against Azurite!");
        }

        // Using 'latest' here often breaks the tests.
        // The version will have to be explicitly managed here for reproducible builds/CI.
        // n.b. for reference https://mcr.microsoft.com/en-us/artifact/mar/azure-storage/azurite/tags
        // n.b. --skipApiVersionCheck is needed because Azurite doesn't yet support the 2026-02-06 API version used by the latest SDK
        azurite = new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:3.35.0")
                .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--skipApiVersionCheck")
                .withExposedPorts(AZURITE_BLOB_PORT);
        azurite.start();

        // Save existing properties to restore after tests
        Arrays.stream(PROPERTY_KEYS).forEach(key -> savedProperties.put(key, System.getProperty(key)));
    }

    @Before
    public void setUpProperties() {
        // Start each test with unset properties
        Arrays.stream(PROPERTY_KEYS).forEach(System::clearProperty);

        String blobEndpoint = "http://" + azurite.getHost() + ":" + azurite.getMappedPort(AZURITE_BLOB_PORT) + "/" + AZURITE_ACCOUNT_NAME;

        switch (configurationType) {
            case BLOB_STORAGE_URI:
                // Configure using individual properties
                System.setProperty("azure.blob_storage_uri", blobEndpoint);
                System.setProperty("azure.account_name", AZURITE_ACCOUNT_NAME);
                System.setProperty("azure.access_key", AZURITE_ACCOUNT_KEY);
                break;
            case CONNECTION_STRING:
                // Configure using connection string; clear individual credential properties
                String connectionString = String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s", AZURITE_ACCOUNT_NAME, AZURITE_ACCOUNT_KEY, blobEndpoint);
                System.setProperty("azure.connection_string", connectionString);
                break;
        }
    }

    @AfterClass
    public static void cleanup() {
        // Restore original properties so that GenuineAZURE_PINGDiscoveryTestCase picks up genuine credentials passed by the user
        for (Map.Entry<String, String> entry : savedProperties.entrySet()) {
            if (entry.getValue() != null) {
                System.setProperty(entry.getKey(), entry.getValue());
            } else {
                System.clearProperty(entry.getKey());
            }
        }

        if (azurite != null) {
            azurite.stop();
        }
    }

    private static boolean isDockerAvailable() {
        try {
            DockerClientFactory.instance().client();
            return true;
        } catch (Throwable ex) {
            return false;
        }
    }
}
