package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetsMigrationListener {
    void resourcesSupplied(List<DataSetResource> dataSetResources);

    default boolean migrationsAboutToStart(List<DataSetResource> sourceDataSetResources) {
        return true;
    }

    void startMigration(DataSetResource dataSetResource);

    void migrationSuccessful(DataSetResource dataSetResource);

    void migrationFailed(DataSetResource dataSetResource, DataSetException e);

    void migrationsFinished(MigrationsResult migrationResult);
}
