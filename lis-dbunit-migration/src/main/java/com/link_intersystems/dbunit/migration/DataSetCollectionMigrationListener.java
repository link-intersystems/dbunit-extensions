package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;

import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetCollectionMigrationListener {
    void successfullyMigrated(DataSetResource dataSetResource);

    void dataSetCollectionMigrationFinished(Map<DataSetResource, DataSetResource> migratedDataSetResources);

    void failedMigration(DataSetResource dataSetResource, DataSetException e);

    void startMigration(DataSetResource dataSetResource);

    void resourcesSupplied(List<DataSetResource> dataSetResources);
}
