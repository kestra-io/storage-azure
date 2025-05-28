package io.kestra.storage.azure;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.assertj.core.api.Assertions.assertThat;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.utils.IdUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@KestraTest(environments = "secrets")
class AzureStorageTest extends StorageTestSuite {

    private static final String CONTENT_STRING = "Content";

    @Test
    void filesByPrefix() throws IOException {
        this.storageInterface.put("main", "namespace", URI.create("/namespace/file.txt"), new ByteArrayInputStream(new byte[0]));
        this.storageInterface.put("tenant", "namespace", URI.create("/namespace/tenant_file.txt"), new ByteArrayInputStream(new byte[0]));
        this.storageInterface.put("main", "namespace", URI.create("/namespace/another_file.json"), new ByteArrayInputStream(new byte[0]));
        this.storageInterface.put("main", "namespace", URI.create("/namespace/folder/file.txt"), new ByteArrayInputStream(new byte[0]));
        this.storageInterface.put("main", "namespace", URI.create("/namespace/folder/some.yaml"), new ByteArrayInputStream(new byte[0]));
        this.storageInterface.put("main", "namespace", URI.create("/namespace/folder/sub/script.py"), new ByteArrayInputStream(new byte[0]));
        List<URI> res = this.storageInterface.allByPrefix("main", "namespace", URI.create("kestra:///namespace/"), false);
        Assertions.assertThat(res).containsExactlyInAnyOrder(new URI[]{URI.create("kestra:///namespace/file.txt"), URI.create("kestra:///namespace/another_file.json"), URI.create("kestra:///namespace/folder/file.txt"), URI.create("kestra:///namespace/folder/some.yaml"), URI.create("kestra:///namespace/folder/sub/script.py")});
        res = this.storageInterface.allByPrefix("tenant", "namespace", URI.create("/namespace"), false);
        Assertions.assertThat(res).containsExactlyInAnyOrder(new URI[]{URI.create("kestra:///namespace/tenant_file.txt")});
        res = this.storageInterface.allByPrefix("main", "namespace", URI.create("/namespace/folder"), false);
        Assertions.assertThat(res).containsExactlyInAnyOrder(new URI[]{URI.create("kestra:///namespace/folder/file.txt"), URI.create("kestra:///namespace/folder/some.yaml"), URI.create("kestra:///namespace/folder/sub/script.py")});
        res = this.storageInterface.allByPrefix("main", "namespace", URI.create("/namespace/folder/sub"), false);
        Assertions.assertThat(res).containsExactlyInAnyOrder(new URI[]{URI.create("kestra:///namespace/folder/sub/script.py")});
        res = this.storageInterface.allByPrefix("main", "namespace", URI.create("/namespace/non-existing"), false);
        Assertions.assertThat(res).isEmpty();
    }

    private URI putFile(String tenantId, String path) throws Exception {
        return storageInterface.put(
            tenantId,
            null,
            new URI(path),
            new ByteArrayInputStream(CONTENT_STRING.getBytes())
        );
    }

}
