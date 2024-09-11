package io.kestra.storage.azure;

import com.azure.identity.DefaultAzureCredentialBuilder;
import io.kestra.core.models.annotations.PluginProperty;

public interface AzureConfig {

    @PluginProperty
    String getEndpoint();

    @PluginProperty
    String getContainer();

    @PluginProperty
    String getConnectionString();

    @PluginProperty
    String getSharedKeyAccountName();

    @PluginProperty
    String getSharedKeyAccountAccessKey();

    @PluginProperty
    String getSasToken();

    /**
     * Specifies the client ID of user assigned or system assigned identity,
     * when this credential is running in an environment with managed identities.
     * <p>
     * If unset, the value in the AZURE_CLIENT_ID environment variable will be used.
     * </p>
     * @see DefaultAzureCredentialBuilder#managedIdentityClientId(String)
     */
    @PluginProperty
    String getManagedIdentityClientId();

    /**
     * Specifies the resource ID of user assigned or system assigned identity,
     * when this credential is running in an environment with managed identities.
     *
     * <p>
     * If unset, the value in the AZURE_CLIENT_ID environment variable will be used.
     * </p>
     * @see DefaultAzureCredentialBuilder#managedIdentityResourceId(String)
     */
    @PluginProperty
    String getManagedIdentityResourceId();
}
