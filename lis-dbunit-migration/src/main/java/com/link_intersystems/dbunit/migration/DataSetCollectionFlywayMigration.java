package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupport;
import com.link_intersystems.io.FileScanner;
import com.link_intersystems.io.PathMatch;
import com.link_intersystems.io.PathMatches;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionMigrationListener migrationListener = new DataSetCollectionFlywayMigrationLogger();

    private DataSetFileDetection dataSetFileDetection = new DataSetFileDetection();
    private TargetPathSupplier targetPathSupplier = new BasepathTargetPathSupplier();
    private FileScanner fileScanner;

    private DatabaseContainerSupport databaseContainerSupport;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;

    public DataSetCollectionFlywayMigration(File basedir) {
        this(basedir.toPath());
    }

    public DataSetCollectionFlywayMigration(Path basepath) {
        fileScanner = new FileScanner(basepath);
    }

    public void setMigrationListener(DataSetCollectionMigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
    }

    public void setBeforeMigration(DataSetTransormer beforeMigrationTransformer) {
        this.beforeMigrationTransformer = beforeMigrationTransformer;
    }

    public void setAfterMigrationTransformer(DataSetTransormer afterMigrationTransformer) {
        this.afterMigrationTransformer = afterMigrationTransformer;
    }

    public void addDefaultFilePatterns() {
        addFilePatterns("**/*.xml", "*.xml", "**/*.xls", "*.xls");
        addDirectoryPatterns("**");
    }

    public void addFilePatterns(String... globPattern) {
        fileScanner.addFilePattern(globPattern);
    }

    public void addDirectoryPatterns(String... globPattern) {
        fileScanner.addDirectoryPatterns(globPattern);
    }

    public void setDataSetFileDetection(DataSetFileDetection dataSetFileDetection) {
        this.dataSetFileDetection = requireNonNull(dataSetFileDetection);
    }

    public void setTargetPathSupplier(TargetPathSupplier targetPathSupplier) {
        this.targetPathSupplier = requireNonNull(targetPathSupplier);
    }

    /**
     * @see com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupportFactory
     */
    public void setDatabaseContainerSupport(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = databaseContainerSupport;
    }

    public void exec() {
        exec(NullProgressListener.INSTANCE);
    }

    public void exec(ProgressListener progressListener) {
        PathMatches dataSetMatches = fileScanner.scan();

        migrationListener.pathScanned(dataSetMatches);

        progressListener.begin(dataSetMatches.size());
        try {
            if (!dataSetMatches.isEmpty()) {
                migrate(progressListener, dataSetMatches);
            }
        } finally {
            progressListener.done();
        }
    }

    private void migrate(ProgressListener progressListener, PathMatches dataSetMatches) {
        if (databaseContainerSupport == null) {
            throw new IllegalStateException("datasetContainerSupport must be set");
        }

        Map<Path, Path> migratedPaths = new LinkedHashMap<>();

        for (PathMatch dataSetMatch : dataSetMatches) {
            Path migratedPath = tryMigrate(dataSetMatch);
            if (migratedPath != null) {
                Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
                migratedPaths.put(sourcePathAbsolute, migratedPath);
            }
            progressListener.worked(1);
        }

        migrationListener.dataSetCollectionMigrationFinished(migratedPaths);
    }

    private Path tryMigrate(PathMatch dataSetMatch) {
        try {
            Path migratedDataSetFile = migrate(dataSetMatch);
            if (migratedDataSetFile != null) {
                migrationListener.successfullyMigrated(migratedDataSetFile);
            } else {
                Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
                migrationListener.skippedMigrationTypeNotDetectable(sourcePathAbsolute);
            }
            return migratedDataSetFile;
        } catch (DataSetException e) {
            Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
            migrationListener.failedMigration(sourcePathAbsolute, e);
            return null;
        }
    }

    private Path migrate(PathMatch dataSetMatch) throws DataSetException {
        Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
        DataSetFile sourceDataSetFile = dataSetFileDetection.detect(sourcePathAbsolute);

        if (sourceDataSetFile == null) {
            return null;
        }

        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
        flywayMigration.setDataSetProducer(sourceDataSetFile.createProducer());
        flywayMigration.setDatabaseContainerSupport(databaseContainerSupport);
        flywayMigration.apply(this);
        flywayMigration.setBeforeMigrationTransformer(beforeMigrationTransformer);
        flywayMigration.setAfterMigrationTransformer(afterMigrationTransformer);


        Path targetDataSetPath = targetPathSupplier.getTarget(dataSetMatch.getPath());
        DataSetFile targetDataSetFile = sourceDataSetFile.withNewPath(targetDataSetPath);

        IDataSetConsumer targetDataSetFileConsumer = targetDataSetFile.createConsumer();
        flywayMigration.setDataSetConsumer(targetDataSetFileConsumer);

        migrationListener.startMigration(sourcePathAbsolute);
        flywayMigration.exec();
        return targetDataSetPath;
    }

}
