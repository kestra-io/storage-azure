package io.kestra.storage.azure;

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.*;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@AzureStorageEnabled
@Introspected
public class AzureStorage implements StorageInterface {
    private static final String DIRECTORY_MARKER_FILE = ".kestradirectory";
    @Inject
    AzureClientFactory factory;

    @Inject
    AzureConfig config;

    private BlobContainerClient client() {
        return factory.client(config);
    }

    private BlobClient blob(URI uri) {
        return this.client().getBlobClient(uri.getPath());
    }

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(getURI(tenantId, uri));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.openInputStream();
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    @Override
    public List<FileAttributes> list(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = (path.endsWith("/")) ? path : path + "/";

        if (!client().getBlobClient(prefix).exists()) {
            throw new FileNotFoundException(uri + " (Not Found)");
        }

        ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
            .setPrefix(prefix)
            .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));

        return this.client().listBlobsByHierarchy("/", listBlobsOptions, null).stream()
            .map(BlobItem::getName)
            .filter(key -> {
                key = "/" + key;
                key = key.substring(prefix.length());
                // Remove recursive result and requested dir
                return !key.isEmpty() && !Objects.equals(key, prefix)
                    && !key.endsWith(DIRECTORY_MARKER_FILE);
            })
            .map(throwFunction(this::getFileAttributes))
            .toList();
    }

    @Override
    public boolean exists(String tenantId, URI uri) {
        try {
            BlobClient blobClient = this.blob(getURI(tenantId, uri));
            return blobClient.exists();
        } catch (BlobStorageException e) {
            return false;
        }
    }

    @Override
    public Long size(String tenantId, URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(getURI(tenantId, uri));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.getProperties().getBlobSize();
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    @Override
    public Long lastModifiedTime(String tenantId, URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(getURI(tenantId, uri));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.getProperties().getLastModified().toInstant().toEpochMilli();
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return getFileAttributes(path);
    }

    private FileAttributes getFileAttributes(String path) throws FileNotFoundException {
        String fileName = Path.of(path).getFileName().toString();

        BlobClient blobClient = this.client().getBlobClient(path);
        if (!path.endsWith("/")) {
            BlobClient dirBlobClient = this.client().getBlobClient(path + "/" + DIRECTORY_MARKER_FILE);
            if (dirBlobClient.exists()) {
                path += "/";
                blobClient = dirBlobClient;
            }
        }
        if (!blobClient.exists()) {
            throw new FileNotFoundException(fileName + " (File not found)");
        }

        return AzureFileAttributes.builder()
            .fileName(fileName)
            .isDirectory(path.endsWith("/") || path.endsWith(DIRECTORY_MARKER_FILE))
            .properties(blobClient.getProperties())
            .build();
    }

    @Override
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        try {
            URI path = getURI(tenantId, uri);
            BlobClient blobClient = this.blob(path);
            mkdirs(path.getPath());
            try (data) {
                blobClient.upload(data, true);
            }

            return URI.create("kestra://" + uri.getPath());
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    public boolean delete(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (isDir(path)) {
            return !deleteByPrefix(tenantId, uri).isEmpty();
        }
        BlobClient blobClient = this.client().getBlobClient(path);
        if (!blobClient.exists()) {
            return false;
        }
        blobClient.delete();
        return true;
    }

    @Override
    public URI createDirectory(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!StringUtils.endsWith(path, "/")) {
            path += "/";
        }
        mkdirs(path + DIRECTORY_MARKER_FILE);
        return URI.create("kestra://" + uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/") && path.lastIndexOf('/') > 0) {
            path = path.substring(0, path.lastIndexOf('/') + 1);
        }

        path += DIRECTORY_MARKER_FILE;

        try {
            BlobClient blobClient = this.blob(URI.create(path));
            blobClient.upload(new ByteArrayInputStream(new byte[]{}), true);
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(URI.create(path), e);
        }
    }

    @Override
    public URI move(String tenantId, URI from, URI to) throws IOException {
        if (!exists(tenantId, from)) {
            throw new FileNotFoundException(from + " (File not found)");
        }

        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);

        BlobContainerClient client = this.client();

        ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
            .setPrefix(getPath(tenantId, from))
            .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));
        for (BlobItem itemResult : client.listBlobs(listBlobsOptions, Duration.ofSeconds(30))) {
            if (isDir(itemResult.getName())) {
                // do not copy directories
                continue;
            }
            String destName = dest + itemResult.getName().substring(source.substring(1).length());
            mkdirs(dest);
            BlobClient sourceClient = client.getBlobClient(itemResult.getName());
            SyncPoller<BlobCopyInfo, Void> poller = client.getBlobClient(destName).beginCopy(sourceClient.getBlobUrl(),
                Duration.ofSeconds(1));
            poller.waitForCompletion();
        }
        deleteByPrefix(tenantId, from);
        return URI.create("kestra://" + from.getPath());
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        try {
            BlobContainerClient client = this.client();

            String path = getPath(tenantId, storagePrefix);
            if (!path.endsWith("/")) {
                path += "/";
            }
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
                .setPrefix(path)
                .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));

            List<String> deleted = new ArrayList<>();
            List<String> directories = new ArrayList<>();
            for (BlobItem itemResult : client.listBlobs(listBlobsOptions, Duration.ofSeconds(30))) {
                String name = itemResult.getName();
                Boolean isDir = isDir(name);
                if (isDir) {
                    directories.add(name);
                    continue;
                }
                BlobClient blobClient = this.client().getBlobClient(name);
                blobClient.delete();
                if (!name.endsWith(DIRECTORY_MARKER_FILE)) {
                    // We do not want to output the hidden file we use to mark directory as deleted
                    deleted.add(name);
                }
            }

            // also remove main directory (we have to remove start and ending /)
            directories.add(path.substring(1, path.length() - 1));
            directories.sort((s1, s2) -> s2.length() - s1.length());
            for (String directory : directories) {
                BlobClient blobClient = this.client().getBlobClient(directory);
                blobClient.delete();
                deleted.add(directory);
            }

            return deleted.stream()
                .map(s -> URI.create("kestra:///" + s.replace(tenantId + "/", "")))
                .toList();
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                return List.of();
            }
            throw new IOException(e);
        }
    }

    private Boolean isDir(String path) {
        // To check if a path is in fact a directory we check if our hidden file exists inside it
        return this.client().getBlobClient(path + "/" + DIRECTORY_MARKER_FILE).exists();
    }

    private IOException reThrowBlobStorageException(URI storagePrefix, BlobStorageException e) {
        if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
            return new FileNotFoundException(storagePrefix + " (File not found)");
        }
        return new IOException(e);
    }

    private URI getURI(String tenantId, URI uri) {
        return URI.create(getPath(tenantId, uri));
    }

    private String getPath(String tenantId, URI uri) {
        if (uri == null) {
            uri = URI.create("/");
        }

        parentTraversalGuard(uri);
        String path = uri.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        if (tenantId == null) {
            return path;
        }
        return "/" + tenantId + path;
    }

    // Traversal does not work with azure but it just return empty objects so throwing is more explicit
    private void parentTraversalGuard(URI uri) {
        if (uri.toString().contains("..")) {
            throw new IllegalArgumentException("File should be accessed with their full path and not using relative '..' path.");
        }
    }
}
