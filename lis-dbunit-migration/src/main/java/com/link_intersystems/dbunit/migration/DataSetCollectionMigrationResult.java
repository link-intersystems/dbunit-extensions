package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionMigrationResult {

    public static final DataSetCollectionMigrationResult EMPTY_RESULT = new DataSetCollectionMigrationResult(Collections.emptyMap());

    private Map<DataSetResource, DataSetResource> migratedDataSetResources;

    DataSetCollectionMigrationResult(Map<DataSetResource, DataSetResource> migratedDataSetResources) {
        this.migratedDataSetResources = requireNonNull(migratedDataSetResources);
    }

    public Map<DataSetResource, DataSetResource> getMigratedDataSetResources() {
        return Collections.unmodifiableMap(migratedDataSetResources);
    }
}
