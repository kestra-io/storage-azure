package io.kestra.storage.azure;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

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
            builder.credential(new DefaultAzureCredentialBuilder().build());
        }

        BlobServiceClient blobServiceClient = builder.buildClient();

        return blobServiceClient.getBlobContainerClient(config.getContainer());
    }
}
