package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetCollectionMigrationListener {
    void resourcesSupplied(List<DataSetResource> dataSetResources);

    void startMigration(DataSetResource dataSetResource);

    void successfullyMigrated(DataSetResource dataSetResource);

    void failedMigration(DataSetResource dataSetResource, DataSetException e);

    void dataSetCollectionMigrationFinished(Map<DataSetResource, DataSetResource> migratedDataSetResources);
}
