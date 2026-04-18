# Kestra Azure Storage

## What

- Implements the storage backend under `io.kestra.storage.azure`.
- Includes classes such as `AzureClientFactory`, `AzureStorage`, `AzureConfig`, `AzureFileAttributes`.

## Why

- This repository implements a Kestra storage backend for storage plugin for Azure Blob Storage.
- It stores namespace files and internal execution artifacts outside local disk.

## How

### Architecture

Single-module plugin.

### Project Structure

```
storage-azure/
├── src/main/java/io/kestra/storage/azure/
├── src/test/java/io/kestra/storage/azure/
├── build.gradle
└── README.md
```

## Local rules

- Keep the scope on Kestra internal storage behavior, not workflow task semantics.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
