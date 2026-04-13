/*
 * Copyright 2015 Red Hat Inc., and individual contributors
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import com.azure.core.util.BinaryData;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.jgroups.Address;
import org.jgroups.annotations.Property;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Responses;

/**
 * Implementation of a {@link org.jgroups.protocols.Discovery} protocol for Microsoft Azure using Blob Service as cluster information store.
 *
 * @author Radoslav Husar
 */
public class AZURE_PING extends FILE_PING {

    private static final Log log = LogFactory.getLog(AZURE_PING.class);

    @Property(description = "The name of the storage account.",
            systemProperty = "JGROUPS_AZURE_STORAGE_ACCOUNT_NAME")
    protected String storage_account_name;

    @Property(description = "The secret account access key. If not specified, DefaultAzureCredential is used instead.",
            systemProperty = "JGROUPS_AZURE_STORAGE_ACCESS_KEY",
            exposeAsManagedAttribute = false)
    protected String storage_access_key;

    @Property(description = "Azure Storage connection string. When set, overrides storage_account_name, storage_access_key, use_https, endpoint_suffix, and blob_storage_uri.",
            systemProperty = "JGROUPS_AZURE_CONNECTION_STRING",
            exposeAsManagedAttribute = false)
    protected String connection_string;

    @Property(description = "Container to store ping information in. Must be a valid DNS name as it becomes part of the Azure blob storage URL.",
            systemProperty = "JGROUPS_AZURE_CONTAINER")
    protected String container;

    @Property(description = "Whether or not to use HTTPS to connect to Azure.",
            systemProperty = "JGROUPS_AZURE_USE_HTTPS")
    protected boolean use_https = true;

    @Property(description = "The endpointSuffix to use.",
            systemProperty = "JGROUPS_AZURE_ENDPOINT_SUFFIX")
    protected String endpoint_suffix = DEFAULT_ENDPOINT_SUFFIX;

    @Property(description = "The full blob service endpoint URI. When set, overrides use_https and endpoint_suffix.",
            systemProperty = "JGROUPS_AZURE_BLOB_STORAGE_URI",
            exposeAsManagedAttribute = false)
    protected String blob_storage_uri;

    private static final String DEFAULT_ENDPOINT_SUFFIX = "core.windows.net";
    private static final String CLUSTER_ADDRESS_FILE_NAME_SEPARATOR = "-";
    public static final int STREAM_BUFFER_SIZE = 4096;

    private BlobContainerClient containerClient;

    static {
        ClassConfigurator.addProtocol((short) 530, AZURE_PING.class);
    }

    public AZURE_PING() {
        super();

        // Disable shutdown hook by default
        this.register_shutdown_hook = false;
    }

    @Override
    public void init() throws Exception {
        super.init();

        // Validate configuration
        // Can throw IAEs
        this.validateConfiguration();

        try {
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

            if (connection_string != null && !connection_string.isEmpty()) {
                builder.connectionString(connection_string);
            } else {
                // Set credential: use shared key if access key is provided, otherwise fall back to DefaultAzureCredential
                if (storage_access_key != null && !storage_access_key.isEmpty()) {
                    builder.credential(new StorageSharedKeyCredential(storage_account_name, storage_access_key));
                } else {
                    try {
                        builder.credential(new DefaultAzureCredentialBuilder().build());
                    } catch (NoClassDefFoundError e) {
                        throw new IllegalStateException("DefaultAzureCredential requires 'com.azure:azure-identity' dependency on the classpath.", e);
                    }
                }

                // Set endpoint
                if (blob_storage_uri != null && !blob_storage_uri.isEmpty()) {
                    builder.endpoint(blob_storage_uri);
                } else {
                    String suffix = (endpoint_suffix != null && !endpoint_suffix.isEmpty()) ? endpoint_suffix : DEFAULT_ENDPOINT_SUFFIX;
                    String protocol = use_https ? "https" : "http";
                    builder.endpoint(String.format("%s://%s.blob.%s", protocol, storage_account_name, suffix));
                }
            }

            BlobServiceClient blobServiceClient = builder.buildClient();
            containerClient = blobServiceClient.getBlobContainerClient(container);
            boolean created = containerClient.createIfNotExists();

            if (created) {
                log.info("Created container named '%s'.", container);
            } else {
                log.debug("Using existing container named '%s'.", container);
            }

        } catch (Exception ex) {
            log.error("Error creating a storage client! Check your configuration.");
            throw ex;
        }
    }

    public void validateConfiguration() throws IllegalArgumentException {
        // Validate that container name is configured and must be all lowercase
        if (container == null || !container.toLowerCase().equals(container) || container.contains("--")
                || container.startsWith("-") || container.length() < 3 || container.length() > 63) {
            throw new IllegalArgumentException("Container name must be configured and must meet Azure requirements (must be a valid DNS name).");
        }
        // Either connection_string or storage_account_name must be configured
        if ((connection_string == null || connection_string.isEmpty()) && (storage_account_name == null || storage_account_name.isEmpty())) {
            throw new IllegalArgumentException("Either connection_string or storage_account_name must be configured.");
        }
        // Let's inform users here that https would be preferred
        if (!use_https && (connection_string == null || connection_string.isEmpty())) {
            log.warn("Configuration is using HTTP, consider switching to HTTPS instead.");
        }

    }

    @Override
    protected void createRootDir() {
        // Do not remove this!
        // There is no root directory to create, overriding here with noop.
    }

    @Override
    protected void readAll(final List<Address> members, final String clustername, final Responses responses) {
        if (clustername == null) {
            return;
        }

        String prefix = getSanitizedPrefix(clustername);

        ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
        for (BlobItem blobItem : containerClient.listBlobs(options, null)) {
            try {
                BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
                ByteArrayOutputStream os = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);
                blobClient.downloadStream(os);
                byte[] pingBytes = os.toByteArray();
                parsePingData(pingBytes, members, responses);
            } catch (Exception t) {
                log.error(String.format("Error fetching/reading ping data file '%s'.", blobItem.getName()), t);
            }
        }
    }

    protected void parsePingData(final byte[] pingBytes, final List<Address> members, final Responses responses) {
        if (pingBytes == null || pingBytes.length <= 0) {
            return;
        }
        List<PingData> list;
        try {
            list = read(new ByteArrayInputStream(pingBytes));
            if (list != null) {
                // This is a common piece of logic for all PING protocols copied from org/jgroups/protocols/FILE_PING.java:245
                // Maybe could be extracted for all PING impls to share this logic?
                for (PingData data : list) {
                    if (members == null || members.contains(data.getAddress())) {
                        responses.addResponse(data, data.isCoord());
                    }
                    if (local_addr != null && !local_addr.equals(data.getAddress())) {
                        addDiscoveryResponseToCaches(data.getAddress(), data.getLogicalName(), data.getPhysicalAddr());
                    }
                }
                // end copied block
            }
        } catch (Exception e) {
            log.error("Error unmarshalling ping data.", e);
        }
    }

    @Override
    protected void write(final List<PingData> list, final String clustername) {
        if (list == null || clustername == null) {
            return;
        }

        String filename = addressToFilename(clustername, local_addr);
        ByteArrayOutputStream out = new ByteArrayOutputStream(STREAM_BUFFER_SIZE);

        try {
            write(list, out);
            byte[] data = out.toByteArray();

            // Upload the file
            BlobClient blobClient = containerClient.getBlobClient(filename);
            blobClient.upload(BinaryData.fromBytes(data), true);

        } catch (Exception ex) {
            log.error("Error marshalling and uploading ping data.", ex);
        }

    }

    @Override
    protected void remove(final String clustername, final Address addr) {
        if (clustername == null || addr == null) {
            return;
        }

        String filename = addressToFilename(clustername, addr);

        try {
            BlobClient blobClient = containerClient.getBlobClient(filename);
            boolean deleted = blobClient.deleteIfExists();

            if (deleted) {
                log.debug("Deleted ping data file '%s'.", filename);
            } else {
                log.debug("Tried to delete ping data file '%s' but it was already deleted.", filename);
            }

        } catch (Exception ex) {
            log.error(String.format("Error deleting ping data file '%s'.", filename), ex);
        }
    }

    @Override
    protected void removeAll(String clustername) {
        if (clustername == null) {
            return;
        }

        String prefix = getSanitizedPrefix(clustername);

        ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
        for (BlobItem blobItem : containerClient.listBlobs(options, null)) {
            try {
                BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
                boolean deleted = blobClient.deleteIfExists();
                if (deleted) {
                    log.debug("Deleted ping data file '%s'.", blobItem.getName());
                } else {
                    log.debug("Tried to delete ping data file '%s' but it was already deleted.", blobItem.getName());
                }
            } catch (Exception e) {
                log.error(String.format("Error deleting ping data file for cluster '%s'.", clustername), e);
            }
        }
    }

    /**
     * Converts cluster name and address into a filename.
     */
    protected static String addressToFilename(final String clustername, final Address address) {
        return getSanitizedPrefix(clustername) + addressToFilename(address);
    }

    /**
     * Sanitizes names replacing backslashes and forward slashes with a dash and appends a separator.
     */
    protected static String getSanitizedPrefix(final String name) {
        return name.replace('/', '-').replace('\\', '-') + CLUSTER_ADDRESS_FILE_NAME_SEPARATOR;
    }


}
