package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionMigrationResult {

    public static final DataSetCollectionMigrationResult EMPTY_RESULT = new DataSetCollectionMigrationResult(Collections.emptyMap());

    private Map<DataSetFile, DataSetFile> migratedDataSetFiles;

    DataSetCollectionMigrationResult(Map<DataSetFile, DataSetFile> migratedDataSetFiles) {
        this.migratedDataSetFiles = requireNonNull(migratedDataSetFiles);
    }

    public Map<DataSetFile, DataSetFile> getMigratedPaths() {
        return Collections.unmodifiableMap(migratedDataSetFiles);
    }
}
