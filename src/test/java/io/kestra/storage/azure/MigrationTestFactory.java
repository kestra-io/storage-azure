package io.kestra.storage.azure;

import io.kestra.core.migration.MigrationHistoryStore;
import io.kestra.core.migration.MigrationLock;
import io.kestra.core.migration.MigrationScript;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/**
 * Provides no-op migration beans for the in-memory test context.
 * The memory backend ships no MigrationLock / MigrationHistoryStore implementation,
 * but MigrationRunner (activated by kestra.repository.type=memory) requires them.
 */
@Factory
class MigrationTestFactory {

    @Singleton
    MigrationLock migrationLock() {
        return new MigrationLock() {
            @Override
            public void acquire() {
            }

            @Override
            public void release() {
            }

            @Override
            public boolean tryAcquire() {
                return true;
            }

            @Override
            public void forceRelease() {
            }
        };
    }

    @Singleton
    MigrationHistoryStore migrationHistoryStore() {
        return new MigrationHistoryStore() {
            @Override
            public void bootstrapIfNeeded() {
            }

            @Override
            public boolean hasAnyApplied() {
                return true;
            }

            @Override
            public boolean isApplied(String scriptId) {
                return true;
            }

            @Override
            public void validateChecksum(MigrationScript script) {
            }

            @Override
            public void markApplied(MigrationScript script, long executionMs) {
            }

            @Override
            public boolean detectLegacyUpgrade() {
                return false;
            }
        };
    }
}
