package io.kestra.storage.azure;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(environments = "secrets")
class AzureStoragePurgeTest {

    private static final String TENANT = "tenant-" + IdUtils.create();

    @Inject
    private StorageInterface storageInterface;

    private URI upload(String prefix, String filename) throws Exception {
        var uri = URI.create("/" + prefix + "/" + filename);
        storageInterface.put(TENANT, null, uri, new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)));
        return URI.create("kestra://" + uri.getPath());
    }

    @Test
    void inWindowFilesAreDeletedAndReturned() throws Exception {
        var prefix = IdUtils.create();
        var before = Instant.now().minusSeconds(5);
        var file1 = upload(prefix, "a.txt");
        var file2 = upload(prefix, "b.txt");
        var after = Instant.now().plusSeconds(5);

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), before, after, false
        );

        assertThat(result, hasSize(2));
        assertThat(result, containsInAnyOrder(file1, file2));
        // files must be gone
        assertThat(storageInterface.exists(TENANT, null, file1), is(false));
        assertThat(storageInterface.exists(TENANT, null, file2), is(false));
    }

    @Test
    void outOfWindowFilesArePreserved() throws Exception {
        var prefix = IdUtils.create();
        upload(prefix, "old.txt");

        // window is in the future — nothing matches
        var future = Instant.now().plusSeconds(60);
        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), future, null, false
        );

        assertThat(result, is(empty()));
        // the file must still exist
        assertThat(storageInterface.exists(TENANT, null, URI.create("kestra:///" + prefix + "/old.txt")), is(true));
    }

    @Test
    void dryRunReturnMatchesWithoutDeleting() throws Exception {
        var prefix = IdUtils.create();
        var file = upload(prefix, "c.txt");
        var before = Instant.now().minusSeconds(5);
        var after = Instant.now().plusSeconds(5);

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), before, after, true
        );

        assertThat(result, hasSize(1));
        assertThat(result, containsInAnyOrder(file));
        // file must still be present after dry-run
        assertThat(storageInterface.exists(TENANT, null, file), is(true));
    }

    @Test
    void missingPrefixReturnsEmptyList() throws Exception {
        var nonexistent = "nonexistent-" + IdUtils.create();

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + nonexistent + "/"), null, null, false
        );

        assertThat(result, is(empty()));
    }

    @Test
    void nullStartBoundMatchesAllBeforeEnd() throws Exception {
        var prefix = IdUtils.create();
        var file = upload(prefix, "d.txt");
        var after = Instant.now().plusSeconds(5);

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), null, after, false
        );

        assertThat(result, hasSize(1));
        assertThat(result, containsInAnyOrder(file));
    }

    @Test
    void nullEndBoundMatchesAllAfterStart() throws Exception {
        var prefix = IdUtils.create();
        var file = upload(prefix, "e.txt");
        var before = Instant.now().minusSeconds(5);

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), before, null, false
        );

        assertThat(result, hasSize(1));
        assertThat(result, containsInAnyOrder(file));
    }

    @Test
    void bothNullBoundsMatchesAll() throws Exception {
        var prefix = IdUtils.create();
        var file1 = upload(prefix, "f.txt");
        var file2 = upload(prefix, "g.txt");

        var result = storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), null, null, false
        );

        assertThat(result, hasSize(2));
        assertThat(result, containsInAnyOrder(file1, file2));
    }

    @Test
    void siblingPrefixNotAffected() throws Exception {
        var prefix = IdUtils.create();
        // upload under prefix/ and prefix-sibling/
        var fileInPrefix = upload(prefix, "h.txt");
        var sibling = prefix + "-sibling";
        var fileInSibling = upload(sibling, "i.txt");
        var before = Instant.now().minusSeconds(5);
        var after = Instant.now().plusSeconds(5);

        storageInterface.purgeByLastModified(
            TENANT, null, URI.create("/" + prefix + "/"), before, after, false
        );

        // sibling must be unaffected
        assertThat(storageInterface.exists(TENANT, null, fileInSibling), is(true));
    }
}
