package io.kestra.storage.azure;

import com.azure.identity.DefaultAzureCredentialBuilder;
import io.kestra.core.models.annotations.PluginProperty;

import java.util.List;

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
     * @see DefaultAzureCredentialBuilder#workloadIdentityClientId(String)
     */
    @PluginProperty
    String getWorkloadIdentityClientId();
}
