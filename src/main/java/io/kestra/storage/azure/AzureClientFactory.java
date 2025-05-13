package io.kestra.storage.azure;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.*;

public final class AzureClientFactory {

    public static BlobContainerClient of(final AzureConfig config) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
            .endpoint(config.getEndpoint());

        if (config.getConnectionString() != null) {
            builder.connectionString(config.getConnectionString());
        } else if (config.getSharedKeyAccountName() != null && config.getSharedKeyAccountAccessKey() != null) {
            builder.credential(new AzureNamedKeyCredential(
                config.getSharedKeyAccountName(),
                config.getSharedKeyAccountAccessKey()
            ));
        } else if (config.getSasToken() != null ) {
            builder.sasToken(config.getSasToken());
        } else {
            builder.credential(getDefaultAzureCredential(config));
        }

        BlobServiceClient blobServiceClient = builder.buildClient();

        return blobServiceClient.getBlobContainerClient(config.getContainer());
    }

    /**
     * Static method for constructing a DefaultAzureCredential for the given config.
     *
     * @param config    The {@link AzureConfig} config.
     * @return  a new {@link DefaultAzureCredential} instance.
     */
    private static DefaultAzureCredential getDefaultAzureCredential(AzureConfig config) {
        DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder();

        //region Configure ManagedIdentityCredential
        if (config.getManagedIdentityClientId() != null) {
            defaultAzureCredentialBuilder = defaultAzureCredentialBuilder
                .managedIdentityClientId(config.getManagedIdentityClientId());
        }

        if (config.getManagedIdentityResourceId() != null) {
            defaultAzureCredentialBuilder
                .managedIdentityResourceId(config.getManagedIdentityResourceId());
        }

        if (config.getAdditionallyAllowedTenants() != null) {
            defaultAzureCredentialBuilder
                .additionallyAllowedTenants(config.getAdditionallyAllowedTenants());
        }

        // endregion

        //region Configure WorkloadIdentityClientId
        if (config.getWorkloadIdentityClientId() != null) {
            defaultAzureCredentialBuilder
                .workloadIdentityClientId(config.getWorkloadIdentityClientId());
        }
        // endregion

        return defaultAzureCredentialBuilder.build();
    }
}
