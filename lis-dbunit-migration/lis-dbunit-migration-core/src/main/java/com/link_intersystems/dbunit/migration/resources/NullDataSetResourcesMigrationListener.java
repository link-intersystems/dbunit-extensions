package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class NullDataSetResourcesMigrationListener implements DataSetResourcesMigrationListener {

    public static final NullDataSetResourcesMigrationListener INSTANCE = new NullDataSetResourcesMigrationListener();

    @Override
    public void startMigration(DataSetResource dataSetResource) {
    }

    @Override
    public void migrationSuccessful(DataSetResource dataSetResource) {
    }

    @Override
    public void migrationFailed(DataSetResource dataSetResource, DataSetException e) {
    }

    @Override
    public void migrationsFinished(MigrationsResult migrationResult) {
    }
}
