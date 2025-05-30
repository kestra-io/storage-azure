package io.kestra.storage.azure;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storage.StorageTestSuite;

@KestraTest(environments = "secrets")
class AzureStorageTest extends StorageTestSuite {
}
