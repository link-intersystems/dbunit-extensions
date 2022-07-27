package com.link_intersystems.dbunit.migration;

import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetCollectionMigrationListener {
    void successfullyMigrated(Path path);

    void skippedMigrationTypeNotDetectable(Path path);

    void dataSetCollectionMigrationFinished(Map<Path, Path> fromToPathMap);

    void failedMigration(Path path, DataSetException e);

    void startMigration(Path path);

    void pathScanned(List<FilePath> pathMatches);
}
