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

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionMigrationListener migrationListener = new LoggingDataSetCollectionMigrationListener();

    private DataSetFileDetection dataSetFileDetection = new DataSetFileDetection();
    private TargetDataSetFileSupplier targetDataSetFileSupplier = new BasepathTargetPathSupplier();

    private DataSetFileLocations dataSetFileLocations = new DataSetFileLocationsScanner();

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

    public void setTargetDataSetFileSupplier(TargetDataSetFileSupplier targetDataSetFileSupplier) {
        this.targetDataSetFileSupplier = requireNonNull(targetDataSetFileSupplier);
    }

    public TargetDataSetFileSupplier getTargetDataSetFileSupplier() {
        return targetDataSetFileSupplier;
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
        List<FilePath> dataSetMatches = dataSetFileLocations.getPaths();
        migrationListener.pathScanned(dataSetMatches);

        List<MigrationDescription> migrationDescriptions = resolveDataSetMigrations(dataSetMatches);

        progressListener.begin(migrationDescriptions.size());
        try {
            return migrate(progressListener, migrationDescriptions);
        } finally {
            progressListener.done();
        }

    }

    @NotNull
    private List<MigrationDescription> resolveDataSetMigrations(List<FilePath> dataSetMatches) {
        List<MigrationDescription> migrationDescriptions = new ArrayList<>();
        Set<DataSetFile> uniqueDataSetFiles = new HashSet<>();

        for (FilePath filePath : dataSetMatches) {
            DataSetFile dataSetFile = getDataSetFileDetection().detect(filePath);
            if (dataSetFile != null) {
                if (uniqueDataSetFiles.add(dataSetFile)) {
                    MigrationDescription migrationDescription = new MigrationDescription(filePath, dataSetFile);
                    migrationDescriptions.add(migrationDescription);
                }
            } else {
                migrationListener.skippedMigrationTypeNotDetectable(filePath);
            }
        }
        return migrationDescriptions;
    }

    private DataSetCollectionMigrationResult migrate(ProgressListener progressListener, List<MigrationDescription> migrationDescriptions) {
        if (getDatabaseContainerSupport() == null) {
            throw new IllegalStateException("datasetContainerSupport must be set");
        }

        Map<DataSetFile, DataSetFile> migratedDataSetFiles = new LinkedHashMap<>();

        for (MigrationDescription migrationDescription : migrationDescriptions) {
            DataSetFile migratedDataSetFile = tryMigrate(migrationDescription);
            if (migratedDataSetFile != null) {
                DataSetFile sourceDataSetFile = migrationDescription.getDataSetFile();
                migratedDataSetFiles.put(sourceDataSetFile, migratedDataSetFile);
            }
            progressListener.worked(1);
        }

        migrationListener.dataSetCollectionMigrationFinished(migratedDataSetFiles);

        return new DataSetCollectionMigrationResult(migratedDataSetFiles);
    }

    protected DataSetFile tryMigrate(MigrationDescription migrationDescription) {
        try {
            DataSetFile migratedDataSetFile = migrate(migrationDescription);
            migrationListener.successfullyMigrated(migratedDataSetFile);
            return migratedDataSetFile;
        } catch (DataSetException e) {
            DataSetFile dataSetFile = migrationDescription.getDataSetFile();
            migrationListener.failedMigration(dataSetFile, e);
            return null;
        }
    }


    protected DataSetFile migrate(MigrationDescription migrationDescription) throws DataSetException {
        DataSetFile sourceDataSetFile = migrationDescription.getDataSetFile();

        DataSetFlywayMigration flywayMigration = createDataSetFlywayMigration(sourceDataSetFile);
        flywayMigration.setDatabaseContainerSupport(getDatabaseContainerSupport());
        flywayMigration.setBeforeMigrationTransformer(getBeforeMigrationTransformer());
        flywayMigration.setAfterMigrationTransformer(getAfterMigrationTransformer());

        TargetDataSetFileSupplier targetDataSetFileSupplier = getTargetDataSetFileSupplier();
        DataSetFile targetDataSetFile = targetDataSetFileSupplier.getTarget(sourceDataSetFile);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetFile.createConsumer();
        flywayMigration.setDataSetConsumer(targetDataSetFileConsumer);

        migrationListener.startMigration(sourceDataSetFile);

        flywayMigration.exec();

        return targetDataSetFile;
    }


    @NotNull
    protected DataSetFlywayMigration createDataSetFlywayMigration(DataSetFile sourceDataSetFile) throws DataSetException {
        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
        flywayMigration.setDataSetProducer(sourceDataSetFile.createProducer());
        flywayMigration.apply(this);
        return flywayMigration;
    }

}
