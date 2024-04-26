package io.kestra.storage.azure;

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
}
