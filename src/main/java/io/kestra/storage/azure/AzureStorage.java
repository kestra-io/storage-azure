package io.kestra.storage.azure;

import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
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
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import jakarta.annotation.Nullable;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

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
    private BlobContainerAsyncClient blobContainerClient;

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * {@inheritDoc}
     **/
    @Override
    public void init() {
        this.blobContainerClient = AzureClientFactory.of(this);
    }

    private BlobAsyncClient blob(URI uri) {
        return this.blobContainerClient.getBlobAsyncClient(uri.getPath());
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        return this.getWithMetadata(tenantId, namespace, uri).inputStream();
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        try {
            BlobAsyncClient blobClient = this.blob(getURI(tenantId, uri));

            if (!block(blobClient.exists())) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            BlobProperties properties = block(blobClient.getProperties());
            AtomicReference<ByteArrayOutputStream> downloadData = new AtomicReference<>(new ByteArrayOutputStream());
            // 20 MB
            int byteArraySizeThreshold = 20 * 1000 * 1024;
            InputStream inputStream = block(blobClient.downloadStream().publishOn(Schedulers.boundedElastic()).reduce(
                InputStream.nullInputStream(),
                (is, data) -> {
                    byte[] array = data.array();
                    if (downloadData.get().size() + array.length > byteArraySizeThreshold) {
                        is = new SequenceInputStream(is, new ByteArrayInputStream(downloadData.get().toByteArray()));
                        downloadData.set(new ByteArrayOutputStream());
                    }

                    try {
                        downloadData.get().write(data.array());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    return is;
                }));

            if (downloadData.get().size() > 0) {
                inputStream = new SequenceInputStream(inputStream, new ByteArrayInputStream(downloadData.get().toByteArray()));
            }
            return new StorageObject(properties.getMetadata(), inputStream);
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
            .map(key -> key.startsWith("/") ? key : "/" + key)
            .map(key -> URI.create("kestra://" + prefixPath + key.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
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
            .setDetails(new BlobListDetails()
                .setRetrieveDeletedBlobs(false)
                .setRetrieveSnapshots(false)
            );

        List<BlobItem> blobItems = recursive ?
            block(this.blobContainerClient.listBlobs(listBlobsOptions, null).collectList()) :
            block(this.blobContainerClient.listBlobsByHierarchy("/", listBlobsOptions).collectList());

        List<String> blobs = blobItems.stream().map(BlobItem::getName).toList();

        return blobs
            .stream()
            .filter(item -> !item.endsWith(DIRECTORY_MARKER_FILE))
            .map(item -> blobs.contains(item + "/" + DIRECTORY_MARKER_FILE) ? item + "/" : item)
            .filter(key -> {
                key = ("/" + key).substring(prefix.length());
                // Remove recursive result and requested dir
                return !key.isEmpty()
                    && !key.equals("/")
                    && (includeDirectories || !key.endsWith("/"));
            });
    }

    @Override
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        try {
            URI uriToCheck = uri;
            if (uri.getPath().endsWith("/")) {
                uriToCheck = uri.resolve(DIRECTORY_MARKER_FILE);
            }
            BlobAsyncClient blobClient = this.blob(getURI(tenantId, uriToCheck));
            return block(blobClient.exists());
        } catch (BlobStorageException e) {
            return false;
        }
    }

    private boolean exists(String path) {
        try {
            if (path.endsWith("/")) {
                path = path + DIRECTORY_MARKER_FILE;
            }
            BlobAsyncClient blobClient = this.blobContainerClient.getBlobAsyncClient(path);
            return block(blobClient.exists());
        } catch (BlobStorageException e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        return getFileAttributes(path);
    }

    private FileAttributes getFileAttributes(String path) throws FileNotFoundException {
        String fileName = Path.of(path).getFileName().toString();

        BlobProperties props = this.getDirProperties(path);
        boolean isFile = props == null;
        if (isFile) {
            BlobAsyncClient blobClient = this.blobContainerClient.getBlobAsyncClient(path);
            if (!block(blobClient.exists())) {
                throw new FileNotFoundException(fileName + " (File not found)");
            }
            props = block(blobClient.getProperties());
        }

        return AzureFileAttributes.builder()
            .fileName(fileName)
            .isDirectory(!isFile)
            .properties(props)
            .build();
    }

    @Override
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        try {
            URI path = getURI(tenantId, uri);
            BlobAsyncClient blobClient = this.blob(path);
            mkdirs(path.getPath());
            try (InputStream data = storageObject.inputStream()) {
                block(blobClient.upload(BinaryData.fromStream(data), true));
            }

            Map<String, String> metadata = storageObject.metadata();
            if (metadata != null && !metadata.isEmpty()) {
                block(blobClient.setMetadata(metadata));
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
        BlobAsyncClient blobClient = this.blobContainerClient.getBlobAsyncClient(path);
        if (!block(blobClient.exists())) {
            return false;
        }
        block(blobClient.delete());
        return true;
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!StringUtils.endsWith(path, "/")) {
            path += "/";
        }
        mkdirs(path);
        return URI.create("kestra://" + uri.getPath());
    }

    private void mkdirs(String path) throws IOException {
        path = path.startsWith("/") ? path : "/" + path;
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
                    BlobAsyncClient blobClient = this.blob(URI.create(aggregatedPath + DIRECTORY_MARKER_FILE));
                    block(blobClient.upload(BinaryData.fromBytes(new byte[]{}), true));
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
        for (BlobItem itemResult : block(this.blobContainerClient.listBlobs(listBlobsOptions).collectList())) {
            if (!itemResult.getName().endsWith(DIRECTORY_MARKER_FILE) && this.dirExists(itemResult.getName())) {
                // do not copy directories
                continue;
            }
            String destName = dest + itemResult.getName().substring(source.substring(1).length());
            mkdirs(dest);
            BlobAsyncClient sourceClient = this.blobContainerClient.getBlobAsyncClient(itemResult.getName());
            SyncPoller<BlobCopyInfo, Void> poller = this.blobContainerClient.getBlobAsyncClient(destName).beginCopy(sourceClient.getBlobUrl(),
                Duration.ofSeconds(1)).getSyncPoller();
            poller.waitForCompletion();
        }
        deleteByPrefix(tenantId, namespace, from);
        return URI.create("kestra://" + from.getPath());
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        try {
            String path = getPath(tenantId, storagePrefix);
            if (!path.endsWith("/")) {
                path += "/";
            }
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions()
                .setPrefix(path)
                .setDetails(new BlobListDetails().setRetrieveDeletedBlobs(false).setRetrieveSnapshots(false));

            List<String> deleted = new ArrayList<>();
            List<String> directories = new ArrayList<>();
            for (BlobItem itemResult : block(this.blobContainerClient.listBlobs(listBlobsOptions).collectList())) {
                String name = itemResult.getName();
                String strippedDirMarker = name.replace("/" + DIRECTORY_MARKER_FILE, "/");
                if (this.dirExists(name) && !name.endsWith(DIRECTORY_MARKER_FILE)) {
                    directories.add(strippedDirMarker);
                    continue;
                }
                BlobAsyncClient blobClient = this.blobContainerClient.getBlobAsyncClient(name);
                block(blobClient.delete());

                if (!name.endsWith(DIRECTORY_MARKER_FILE)) {
                    deleted.add(name);
                }
            }

            // also remove main directory (we have to remove start and ending /)
            directories.add(path.substring(1, path.length() - 1));
            directories.sort((s1, s2) -> s2.length() - s1.length());
            for (String directory : directories) {
                BlobAsyncClient blobClient = this.blobContainerClient.getBlobAsyncClient(directory);
                block(blobClient.delete());
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

    private boolean dirExists(String path) {
        String dirPath = toDirPath(path);
        return block(this.blobContainerClient.getBlobAsyncClient(dirPath).exists());
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
            return block(this.blobContainerClient.getBlobAsyncClient(this.toDirPath(path)).getProperties());
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

    // Using the blocking client generates error when working with files of more than 1MB,
    // see https://github.com/Azure/azure-sdk-for-java/issues/42268.
    // To work around that, we use the async client and block in another thread (from virtual thread executor).
    // Not nice but while waiting for a fix upstream, it fixes https://github.com/kestra-io/kestra/issues/5383
    private <T> T block(Mono<T> mono) {
        try {
            return executorService.submit(() -> mono.blockOptional().orElse(null)).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e);
        }
    }
}
