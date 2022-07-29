package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionMigrationResult {

    private Map<DataSetResource, DataSetResource> migratedDataSetResources;

    DataSetCollectionMigrationResult(Map<DataSetResource, DataSetResource> migratedDataSetResources) {
        this.migratedDataSetResources = requireNonNull(migratedDataSetResources);
    }

    public Map<DataSetResource, DataSetResource> getMigratedDataSetResources() {
        return Collections.unmodifiableMap(migratedDataSetResources);
    }
}
