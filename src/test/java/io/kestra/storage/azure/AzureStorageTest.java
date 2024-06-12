package io.kestra.storage.azure;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.junit.annotations.KestraTest;

@KestraTest(environments = "secrets")
class AzureStorageTest extends StorageTestSuite {
}
