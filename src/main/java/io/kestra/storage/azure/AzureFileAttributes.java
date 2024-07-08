package io.kestra.storage.azure;

import com.azure.storage.blob.models.BlobProperties;
import io.kestra.core.storages.FileAttributes;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class AzureFileAttributes implements FileAttributes {

    String fileName;
    BlobProperties properties;
    boolean isDirectory;

    @Override
    public long getLastModifiedTime() {
        return properties.getLastModified().toEpochSecond();
    }

    @Override
    public long getCreationTime() {
        return properties.getCreationTime().toEpochSecond();
    }

    @Override
    public FileType getType() {
        if (isDirectory) {
            return FileType.Directory;
        }
        return FileType.File;
    }

    @Override
    public long getSize() {
        return properties.getBlobSize();
    }

    @Override
    public Map<String, String> getMetadata() {
        return properties.getMetadata();
    }
}
