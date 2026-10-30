package io.kestra.storage.azure;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

@KestraTest
class AzureStorageSchemaTest {
    @Inject
    private JsonSchemaGenerator jsonSchemaGenerator;

    @SuppressWarnings("unchecked")
    @Test
    void schema() {
        Map<String, Object> schema = jsonSchemaGenerator.schemas(AzureStorage.class, false);

        Map<String, Object> definitions = (Map<String, Object>) schema.get("definitions");
        Map<String, Object> properties = (Map<String, Object>) ((Map<String, Object>) definitions.get(AzureStorage.class.getName())).get("properties");
        assertThat(properties, hasKey("connectionString"));
        assertThat(properties, not(hasKey("blobContainerClient")));
    }
}
