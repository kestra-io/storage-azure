package io.kestra.storage.azure;

import io.micronaut.context.annotation.Requires;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PACKAGE, ElementType.TYPE})
@Requires(property = "kestra.storage.type", value = "azure")
public @interface AzureStorageEnabled {
}
