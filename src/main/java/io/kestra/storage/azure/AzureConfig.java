package io.kestra.storage.azure;

import java.util.List;

import com.azure.identity.DefaultAzureCredentialBuilder;

import io.kestra.core.models.annotations.PluginProperty;

public interface AzureConfig {

    @PluginProperty(group = "connection")
    String getEndpoint();

    @PluginProperty(group = "advanced")
    String getContainer();

    @PluginProperty(group = "connection")
    String getConnectionString();

    @PluginProperty(group = "advanced")
    String getSharedKeyAccountName();

    @PluginProperty(group = "connection")
    String getSharedKeyAccountAccessKey();

    @PluginProperty(group = "connection")
    String getSasToken();

    /**
     * Specifies the client ID of user assigned or system assigned identity,
     * when this credential is running in an environment with managed identities.
     * <p>
     * If unset, the value in the AZURE_CLIENT_ID environment variable will be used.
     * </p>
     * 
     * @see DefaultAzureCredentialBuilder#managedIdentityClientId(String)
     */
    @PluginProperty(group = "advanced")
    String getManagedIdentityClientId();

    /**
     * Specifies the resource ID of user assigned or system assigned identity,
     * when this credential is running in an environment with managed identities.
     *
     * <p>
     * If unset, the value in the AZURE_CLIENT_ID environment variable will be used.
     * </p>
     * 
     * @see DefaultAzureCredentialBuilder#managedIdentityResourceId(String)
     */
    @PluginProperty(group = "advanced")
    String getManagedIdentityResourceId();

    /**
     * For multi-tenant applications, specifies additional tenants for which the credential may acquire tokens.
     * Add the wildcard value "*" to allow the credential to acquire tokens for any tenant the application is installed.
     *
     * @see DefaultAzureCredentialBuilder#additionallyAllowedTenants(List)
     */
    List<String> getAdditionallyAllowedTenants();

    /**
     * Specifies the client ID of Microsoft Entra app to be used for AKS workload identity authentication.
     *
     * <p>
     * If unset, the value in the AZURE_CLIENT_ID environment variable will be used.
     * </p>
     * 
     * @see DefaultAzureCredentialBuilder#workloadIdentityClientId(String)
     */
    @PluginProperty(group = "advanced")
    String getWorkloadIdentityClientId();
}
