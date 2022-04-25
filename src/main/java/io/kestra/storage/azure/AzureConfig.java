package io.kestra.storage.azure;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;

import jakarta.inject.Singleton;

@Singleton
@Getter
@ConfigurationProperties("kestra.storage.azure")
public class AzureConfig {
    protected String endpoint;

    protected String container;

    protected String connectionString;

    protected String sharedKeyAccountName;

    protected String sharedKeyAccountAccessKey;

    protected String sasToken;
}
