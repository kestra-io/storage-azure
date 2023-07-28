package io.kestra.storage.azure;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.collect.Streams;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.core.annotation.Introspected;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@AzureStorageEnabled
@Introspected
public class AzureStorage implements StorageInterface {
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
    public InputStream get(URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.openInputStream();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(URI uri) {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));
            return blobClient.exists();
        } catch (BlobStorageException e) {
            return false;
        }
    }

    @Override
    public Long size(URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.getProperties().getBlobSize();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Long lastModifiedTime(URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));

            if (!blobClient.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blobClient.getProperties().getLastModified().toInstant().toEpochMilli();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public URI put(URI uri, InputStream data) throws IOException {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));

            try (data) {
                blobClient.upload(data, true);
            }

            return URI.create("kestra://" + uri.getPath());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    public boolean delete(URI uri) throws IOException {
        try {
            BlobClient blobClient = this.blob(URI.create(uri.getPath()));

            if (!blobClient.exists()) {
                return false;
            }

            blobClient.delete();

            return true;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
        try {
            BlobContainerClient client = this.client();

            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(storagePrefix.getPath());

            List<String> deleted = Streams
                .stream(client.listBlobs(listBlobsOptions, Duration.ofSeconds(30)))
                .filter(blobItem -> blobItem.getProperties().getContentType() != null)
                .map(throwFunction(itemResult -> {
                    try {
                        return itemResult.getName();
                    } catch (Throwable e) {
                        throw new IOException(e);
                    }
                }))
                .collect(Collectors.toList());

            deleted
                .forEach(s -> {
                    BlobClient blobClient = this.blob(URI.create(s));
                    blobClient.delete();
                });


            return deleted
                .stream()
                .map(s -> URI.create("kestra:///" + s))
                .collect(Collectors.toList());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
