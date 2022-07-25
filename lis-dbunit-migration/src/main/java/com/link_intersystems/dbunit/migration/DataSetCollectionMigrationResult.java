package com.link_intersystems.dbunit.migration;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionMigrationResult {

    public static final DataSetCollectionMigrationResult EMPTY_RESULT = new DataSetCollectionMigrationResult(Collections.emptyMap());

    private Map<Path, Path> migratedPaths;

    DataSetCollectionMigrationResult(Map<Path, Path> migratedPaths) {
        this.migratedPaths = requireNonNull(migratedPaths);
    }

    public Map<Path, Path> getMigratedPaths() {
        return Collections.unmodifiableMap(migratedPaths);
    }
}
