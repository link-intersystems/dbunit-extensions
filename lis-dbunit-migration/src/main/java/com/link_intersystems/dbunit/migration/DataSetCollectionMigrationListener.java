package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetCollectionMigrationListener {
    void successfullyMigrated(DataSetFile dataSetFile);

    void skippedMigrationTypeNotDetectable(FilePath path);

    void dataSetCollectionMigrationFinished(Map<DataSetFile, DataSetFile> fromToDataSetFileMap);

    void failedMigration(DataSetFile dataSetFile, DataSetException e);

    void startMigration(DataSetFile dataSetFile);

    void pathScanned(List<FilePath> pathMatches);
}
