package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetResourcesMigrationListener {

    void startMigration(DataSetResource dataSetResource);

    void migrationSuccessful(DataSetResource dataSetResource);

    void migrationFailed(DataSetResource dataSetResource, DataSetException e);

    void migrationsFinished(MigrationsResult migrationResult);
}
