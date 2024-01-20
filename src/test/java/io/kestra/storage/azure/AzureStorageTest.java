package io.kestra.storage.azure;

import io.kestra.core.storage.StorageTestSuite;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;

@MicronautTest(environments = "secrets")
class AzureStorageTest extends StorageTestSuite {
}
