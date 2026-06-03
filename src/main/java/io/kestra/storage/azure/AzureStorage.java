package io.kestra.storage.azure;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.commons.lang3.Strings;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Builder
@Jacksonized
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Plugin
@Plugin.Id("azure")
public class AzureStorage implements AzureConfig, StorageInterface {

    private static final String DIRECTORY_MARKER_FILE = ".kestradirectory";

    private String endpoint;

    private String container;

    private String connectionString;

    private String sharedKeyAccountName;

    private String sharedKeyAccountAccessKey;

    private String sasToken;

    private String managedIdentityClientId;

    private String managedIdentityResourceId;

    private List<String> additionallyAllowedTenants;

    private String workloadIdentityClientId;

    @Getter(AccessLevel.PRIVATE)
    private BlobContainerClient blobContainerClient;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * {@inheritDoc}
     **/
    @Override
    public void init() {
        this.blobContainerClient = AzureClientFactory.of(this);
    }

    private BlobClient blob(URI uri) {
        return this.blobContainerClient.getBlobClient(uri.getPath());
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(tenantId, namespace, uri).inputStream();
    }

    @Override
    public InputStream getInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(uri, getURI(uri)).inputStream();
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        URI blobUri = getURI(tenantId, uri);
        return getWithMetadata(uri, blobUri);
    }

    private StorageObject getWithMetadata(URI uri, URI blobUri) throws IOException {
        try {
            BlobClient blobClient = this.blob(blobUri);

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            BlobProperties properties = blobClient.getProperties();

            return new StorageObject(properties.getMetadata(), blobClient.openInputStream());
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        String prefixPath = prefix.getPath();
        Stream<String> allKeys = keysForPrefix(path, true, includeDirectories);
        return allKeys
            .map(key -> key.replaceFirst("^/", ""))
            .map(key -> URI.create("kestra://" + prefixPath + key.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return list(uri, path);
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return list(uri, getPath(uri));
    }

    private List<FileAttributes> list(URI uri, String path)
        throws FileNotFoundException {
        String prefix = (path.endsWith("/")) ? path : path + "/";

        if (!this.dirExists(path)) {
            throw new FileNotFoundException(uri + " (Not Found)");
        }

        return keysForPrefix(prefix, false, true)
            .map(throwFunction(this::getFileAttributes))
            .toList();
    }

    private Stream<String> keysForPrefix(String prefix, boolean recursive, boolean includeDirectories) {
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
            .setPrefix(prefix)
            .setDetails(
                new BlobListDetails()
                    .setRetrieveDeletedBlobs(false)
                    .setRetrieveSnapshots(false)
            );

        PagedIterable<BlobItem> blobItems = recursive ? this.blobContainerClient.listBlobs(listBlobsOptions, null) : this.blobContainerClient.listBlobsByHierarchy("/", listBlobsOptions, null);

        List<String> blobs = blobItems.stream().map(BlobItem::getName).toList();

        return blobs
            .stream()
            //Azure may not give folders in the blobs, so we need to get them from the .kestradirectory
            .map(item -> Strings.CS.removeEnd(item, DIRECTORY_MARKER_FILE))
            .map(item -> blobs.contains(item + "/" + DIRECTORY_MARKER_FILE) ? item + "/" : item)
            .filter(key ->
            {
                key = key.substring(prefix.length());
                // Remove recursive result and requested dir
                return !key.isEmpty()
                    && !key.equals("/")
                    && (includeDirectories || !key.endsWith("/"));
            })
            .distinct();
    }

    @Override
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        URI uriToCheck = uri;
        if (uri.getPath().endsWith("/")) {
            uriToCheck = uri.resolve(DIRECTORY_MARKER_FILE);
        }
        URI path = getURI(tenantId, uriToCheck);
        return exist(path);
    }

    @Override
    public boolean existsInstanceResource(@Nullable String namespace, URI uri) {
        URI uriToCheck = uri;
        if (uri.getPath().endsWith("/")) {
            uriToCheck = uri.resolve(DIRECTORY_MARKER_FILE);
        }
        URI path = getURI(uriToCheck);
        return exist(path);
    }

    private boolean exist(URI path) {
        try {
            BlobClient blobClient = this.blob(path);
            return blobClient.exists();
        } catch (BlobStorageException e) {
            return false;
        }
    }

    private boolean exists(String path) {
        try {
            if (path.endsWith("/")) {
                path = path + DIRECTORY_MARKER_FILE;
            }
            BlobClient blobClient = this.blobContainerClient.getBlobClient(path);
            return blobClient.exists();
        } catch (BlobStorageException e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return getFileAttributes(path);
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        return getFileAttributes(getPath(uri));
    }

    private FileAttributes getFileAttributes(String path) throws FileNotFoundException {
        String fileName = Path.of(path).getFileName().toString();

        BlobProperties props = this.getDirProperties(path);
        boolean isFile = props == null;
        if (isFile) {
            BlobClient blobClient = this.blobContainerClient.getBlobClient(path);
            if (!blobClient.exists()) {
                throw new FileNotFoundException(fileName + " (File not found)");
            }
            props = blobClient.getProperties();
        }

        return AzureFileAttributes.builder()
            .fileName(fileName)
            .isDirectory(!isFile)
            .properties(props)
            .build();
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        URI path = getURI(tenantId, uri);
        return put(uri, storageObject, path);
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        return put(uri, storageObject, getURI(uri));
    }

    private URI put(URI uri, StorageObject storageObject, URI path) throws IOException {
        try {
            BlobClient blobClient = this.blob(path);
            mkdirs(path.getPath());
            try (InputStream data = storageObject.inputStream()) {
                blobClient.upload(data, true);
            }

            Map<String, String> metadata = storageObject.metadata();
            if (metadata != null && !metadata.isEmpty()) {
                blobClient.setMetadata(metadata);
            }

            return URI.create("kestra://" + uri.getPath());
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(uri, e);
        }
    }

    @Override
    public boolean delete(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (this.dirExists(path)) {
            return !deleteByPrefix(tenantId, namespace, uri).isEmpty();
        }
        BlobClient blobClient = this.blobContainerClient.getBlobClient(path);
        if (!blobClient.exists()) {
            return false;
        }
        blobClient.delete();
        return true;
    }

    @Override
    public boolean deleteInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        if (this.dirExists(path)) {
            return !deleteByPrefix(null, path).isEmpty();
        }
        BlobClient blobClient = this.blobContainerClient.getBlobClient(path);
        if (!blobClient.exists()) {
            return false;
        }
        blobClient.delete();
        return true;
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return createDirectory(uri, path);
    }

    @Override
    public URI createInstanceDirectory(String namespace, URI uri) throws IOException {
        return createDirectory(uri, getPath(uri));
    }

    private URI createDirectory(URI uri, String path) throws IOException {
        if (!Strings.CS.endsWith(path, "/")) {
            path += "/";
        }
        mkdirs(path);
        return URI.create("kestra://" + uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        if (!path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/") + 1);
        }

        // check if it exists before creating it
        if (exists(path)) {
            return;
        }
        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder();
        try {
            // perform 1 put request per parent directory in the path
            for (String directory : directories) {
                aggregatedPath.append(directory).append("/");
                if (!this.dirExists(aggregatedPath.toString())) {
                    BlobClient blobClient = this.blob(URI.create(aggregatedPath + DIRECTORY_MARKER_FILE));
                    blobClient.upload(BinaryData.fromBytes(new byte[] {}), true);
                }
            }
        } catch (BlobStorageException e) {
            throw reThrowBlobStorageException(URI.create(path), e);
        }
    }

    @Override
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        if (!exists(tenantId, namespace, from)) {
            throw new FileNotFoundException(from + " (File not found)");
        }

        String source = getPath(tenantId, from);
        String dest = getPath(tenantId, to);

        ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
            .setPrefix(getPath(tenantId, from))
            .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));
        for (BlobItem itemResult : this.blobContainerClient.listBlobs(listBlobsOptions, null)) {
            if (!itemResult.getName().endsWith(DIRECTORY_MARKER_FILE) && this.dirExists(itemResult.getName())) {
                // do not copy directories
                continue;
            }
            String destName = dest + itemResult.getName().substring(source.length());
            mkdirs(dest);
            BlobClient sourceClient = this.blobContainerClient.getBlobClient(itemResult.getName());
            SyncPoller<BlobCopyInfo, Void> poller = this.blobContainerClient.getBlobClient(destName).beginCopy(
                sourceClient.getBlobUrl(),
                Duration.ofSeconds(1)
            );
            poller.waitForCompletion();
        }
        deleteByPrefix(tenantId, namespace, from);
        return URI.create("kestra://" + from.getPath());
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        String path = getPath(tenantId, storagePrefix);
        return deleteByPrefix(tenantId, path);
    }

    private List<URI> deleteByPrefix(String tenantId, String path) throws IOException {
        try {
            if (!path.endsWith("/")) {
                path += "/";
            }
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
                .setPrefix(path)
                .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));

            List<String> deleted = new ArrayList<>();
            List<String> directories = new ArrayList<>();
            for (BlobItem itemResult : this.blobContainerClient.listBlobs(listBlobsOptions, null)) {
                String name = itemResult.getName();
                String strippedDirMarker = name.replace("/" + DIRECTORY_MARKER_FILE, "/");
                if (this.dirExists(name) && !name.endsWith(DIRECTORY_MARKER_FILE)) {
                    directories.add(strippedDirMarker);
                    continue;
                }
                BlobClient blobClient = this.blobContainerClient.getBlobClient(name);
                blobClient.delete();

                if (!name.endsWith(DIRECTORY_MARKER_FILE)) {
                    deleted.add(name);
                }
            }

            // also remove main directory (we have to remove ending /)
            directories.add(path.substring(0, path.length() - 1));
            directories.sort((s1, s2) -> s2.length() - s1.length());
            for (String directory : directories) {
                BlobClient blobClient = this.blobContainerClient.getBlobClient(directory);
                blobClient.delete();
                deleted.add(directory);
            }

            return deleted.stream()
                .map(s -> URI.create("kestra:///" + s.replaceFirst(tenantId + "/", "")))
                .toList();
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                return List.of();
            }
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> purgeByLastModified(
        String tenantId,
        @Nullable String namespace,
        URI prefix,
        @Nullable Instant startDate,
        @Nullable Instant endDate,
        boolean dryRun) throws IOException {
        String resolvedPath = getPath(tenantId, prefix);
        if (!resolvedPath.endsWith("/")) {
            resolvedPath += "/";
        }
        final String blobPrefix = resolvedPath;
        String prefixPath = prefix.getPath();

        ListBlobsOptions options = new ListBlobsOptions()
            .setPrefix(blobPrefix)
            .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));

        List<URI> matched = new ArrayList<>();
        try {
            for (BlobItem item : this.blobContainerClient.listBlobs(options, null)) {
                String name = item.getName();
                // skip directory markers — purge targets files only
                if (name.endsWith("/" + DIRECTORY_MARKER_FILE) || name.endsWith(DIRECTORY_MARKER_FILE)) {
                    continue;
                }
                if (isInWindow(item, startDate, endDate)) {
                    matched.add(URI.create("kestra://" + prefixPath + name.substring(blobPrefix.length())));
                }
            }
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                return List.of();
            }
            throw new IOException(e);
        }

        if (dryRun || matched.isEmpty()) {
            return matched;
        }

        var pool = Executors.newFixedThreadPool(16);
        try {
            List<Future<Void>> futures = matched.stream()
                .<Future<Void>> map(uri -> pool.submit(() ->
                {
                    deleteBlobByStoragePath(uri, prefixPath, blobPrefix);
                    return null;
                }))
                .toList();
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (java.util.concurrent.ExecutionException e) {
                    var cause = e.getCause();
                    if (cause instanceof IOException ioe) {
                        throw ioe;
                    }
                    throw new IOException("Failed to delete blob during purge", cause);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Purge interrupted", e);
                }
            }
        } finally {
            pool.shutdown();
        }

        return matched;
    }

    private boolean isInWindow(BlobItem item, @Nullable Instant startDate, @Nullable Instant endDate) {
        var lastModified = item.getProperties().getLastModified();
        if (lastModified == null) {
            return false;
        }
        var ts = lastModified.toInstant();
        return (startDate == null || !ts.isBefore(startDate))
            && (endDate == null || !ts.isAfter(endDate));
    }

    private void deleteBlobByStoragePath(URI kestraUri, String prefixPath, String blobPrefix) throws IOException {
        // reconstruct blob name: blobPrefix + suffix part of the kestra URI
        String suffix = kestraUri.getPath().substring(prefixPath.length());
        String blobName = blobPrefix + suffix;
        try {
            this.blobContainerClient.getBlobClient(blobName).delete();
        } catch (BlobStorageException e) {
            // already gone — treat as success
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                return;
            }
            throw new IOException(e);
        }
    }

    private boolean dirExists(String path) {
        String dirPath = toDirPath(path);
        if (path.isEmpty() || "/".equals(path)) {
            // root == container
            return blobContainerClient.exists();
        }
        return this.blobContainerClient.getBlobClient(dirPath).exists();
    }

    private String toDirPath(String path) {
        String dirPath = path;
        if (!path.endsWith(DIRECTORY_MARKER_FILE)) {
            dirPath = path + (path.endsWith("/") ? "" : "/") + DIRECTORY_MARKER_FILE;
        }
        return dirPath;
    }

    private BlobProperties getDirProperties(String path) {
        try {
            return this.blobContainerClient.getBlobClient(this.toDirPath(path)).getProperties();
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                return null;
            }
            throw e;
        }
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

    private URI getURI(URI uri) {
        return URI.create(getPath(uri));
    }
}
