package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupport;
import com.link_intersystems.io.FilePath;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionMigrationListener migrationListener = new DataSetCollectionFlywayMigrationLogger();

    private DataSetFileDetection dataSetFileDetection = new DataSetFileDetection();
    private TargetPathSupplier targetPathSupplier = new BasepathTargetPathSupplier();

    private DataSetFileLocations dataSetFileLocations = new DataSetFileLocationsScanner();
    ;

    private DatabaseContainerSupport databaseContainerSupport;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;

    public void setMigrationListener(DataSetCollectionMigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
    }

    public void setDataSetFileLocations(DataSetFileLocations dataSetFileLocations) {
        this.dataSetFileLocations = requireNonNull(dataSetFileLocations);
    }

    public void setBeforeMigration(DataSetTransormer beforeMigrationTransformer) {
        this.beforeMigrationTransformer = beforeMigrationTransformer;
    }

    public DataSetTransormer getBeforeMigrationTransformer() {
        return beforeMigrationTransformer;
    }

    public void setAfterMigrationTransformer(DataSetTransormer afterMigrationTransformer) {
        this.afterMigrationTransformer = afterMigrationTransformer;
    }

    public DataSetTransormer getAfterMigrationTransformer() {
        return afterMigrationTransformer;
    }


    public void setDataSetFileDetection(DataSetFileDetection dataSetFileDetection) {
        this.dataSetFileDetection = requireNonNull(dataSetFileDetection);
    }

    public DataSetFileDetection getDataSetFileDetection() {
        return dataSetFileDetection;
    }

    public void setTargetPathSupplier(TargetPathSupplier targetPathSupplier) {
        this.targetPathSupplier = requireNonNull(targetPathSupplier);
    }

    public TargetPathSupplier getTargetPathSupplier() {
        return targetPathSupplier;
    }

    /**
     * @see com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupportFactory
     */
    public void setDatabaseContainerSupport(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = databaseContainerSupport;
    }

    public DatabaseContainerSupport getDatabaseContainerSupport() {
        return databaseContainerSupport;
    }

    public DataSetCollectionMigrationResult exec() {
        return exec(NullProgressListener.INSTANCE);
    }

    public DataSetCollectionMigrationResult exec(ProgressListener progressListener) {
        DataSetCollectionMigrationResult result = DataSetCollectionMigrationResult.EMPTY_RESULT;

        List<FilePath> dataSetMatches = dataSetFileLocations.getPaths();

        migrationListener.pathScanned(dataSetMatches);

        progressListener.begin(dataSetMatches.size());
        try {
            if (!dataSetMatches.isEmpty()) {
                result = migrate(progressListener, dataSetMatches);
            }
        } finally {
            progressListener.done();
        }

        return result;
    }

    private DataSetCollectionMigrationResult migrate(ProgressListener progressListener, List<FilePath> dataSetMatches) {
        if (getDatabaseContainerSupport() == null) {
            throw new IllegalStateException("datasetContainerSupport must be set");
        }

        Map<Path, Path> migratedPaths = new LinkedHashMap<>();

        for (FilePath dataSetMatch : dataSetMatches) {
            Path migratedPath = tryMigrate(dataSetMatch);
            if (migratedPath != null) {
                Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
                migratedPaths.put(sourcePathAbsolute, migratedPath);
            }
            progressListener.worked(1);
        }

        migrationListener.dataSetCollectionMigrationFinished(migratedPaths);

        return new DataSetCollectionMigrationResult(migratedPaths);
    }

    protected Path tryMigrate(FilePath dataSetMatch) {
        try {
            Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
            DataSetFile sourceDataSetFile = getDataSetFileDetection().detect(sourcePathAbsolute);

            if (sourceDataSetFile == null) {
                migrationListener.skippedMigrationTypeNotDetectable(sourcePathAbsolute);
                return null;
            } else {
                Path migratedDataSetFile = migrate(dataSetMatch, sourceDataSetFile);

                if (migratedDataSetFile != null) {
                    migrationListener.successfullyMigrated(migratedDataSetFile);
                } else {
                    migrationListener.skippedMigrationTypeNotDetectable(sourcePathAbsolute);
                }

                return migratedDataSetFile;
            }
        } catch (DataSetException e) {
            Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
            migrationListener.failedMigration(sourcePathAbsolute, e);
            return null;
        }
    }

    protected Path migrate(FilePath dataSetMatch, DataSetFile sourceDataSetFile) throws DataSetException {
        DataSetFlywayMigration flywayMigration = createDataSetFlywayMigration(sourceDataSetFile);
        flywayMigration.setDatabaseContainerSupport(getDatabaseContainerSupport());
        flywayMigration.setBeforeMigrationTransformer(getBeforeMigrationTransformer());
        flywayMigration.setAfterMigrationTransformer(getAfterMigrationTransformer());

        Path targetDataSetPath = getTargetPathSupplier().getTarget(dataSetMatch.getPath());
        IDataSetConsumer targetDataSetConsumer = createTargetDataSetConsumer(sourceDataSetFile, targetDataSetPath);
        flywayMigration.setDataSetConsumer(targetDataSetConsumer);

        Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
        migrationListener.startMigration(sourcePathAbsolute);

        flywayMigration.exec();

        return targetDataSetPath;
    }

    protected IDataSetConsumer createTargetDataSetConsumer(DataSetFile sourceDataSetFile, Path targetDataSetPath) throws DataSetException {
        DataSetFile targetDataSetFile = sourceDataSetFile.withNewPath(targetDataSetPath);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetFile.createConsumer();
        return targetDataSetFileConsumer;
    }

    @NotNull
    protected DataSetFlywayMigration createDataSetFlywayMigration(DataSetFile sourceDataSetFile) throws DataSetException {
        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
        flywayMigration.setDataSetProducer(sourceDataSetFile.createProducer());
        flywayMigration.apply(this);
        return flywayMigration;
    }

}
