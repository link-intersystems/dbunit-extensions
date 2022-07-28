package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.DataSetResourcesSupplier;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupport;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionMigrationListener migrationListener = new LoggingDataSetCollectionMigrationListener();

    private TargetDataSetResourceSupplier targetDataSetFileSupplier;

    private DataSetResourcesSupplier dataSetResourcesSupplier;
    private DatabaseContainerSupport databaseContainerSupport;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;

    public void setDataSetResourcesSupplier(DataSetResourcesSupplier dataSetResourcesSupplier) {
        this.dataSetResourcesSupplier = dataSetResourcesSupplier;
    }

    public DataSetResourcesSupplier getDataSetResourcesSupplier() {
        return dataSetResourcesSupplier;
    }

    public void setMigrationListener(DataSetCollectionMigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
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

    public void setTargetDataSetFileSupplier(TargetDataSetResourceSupplier targetDataSetFileSupplier) {
        this.targetDataSetFileSupplier = requireNonNull(targetDataSetFileSupplier);
    }

    public TargetDataSetResourceSupplier getTargetDataSetFileSupplier() {
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
        DataSetResourcesSupplier dataSetResourcesSupplier = getDataSetResourcesSupplier();
        if (dataSetResourcesSupplier == null) {
            throw new IllegalStateException("dataSetResourcesSupplier must be set");
        }

        List<DataSetResource> sourceDataSetResources = dataSetResourcesSupplier.getDataSetResources();
        migrationListener.resourcesSupplied(sourceDataSetResources);

        progressListener.begin(sourceDataSetResources.size());
        try {
            return migrate(progressListener, sourceDataSetResources);
        } finally {
            progressListener.done();
        }

    }

    private DataSetCollectionMigrationResult migrate(ProgressListener progressListener, List<DataSetResource> sourceDataSetResources) {
        checkMigrationPreconditions();

        Map<DataSetResource, DataSetResource> migratedDataSetFiles = new LinkedHashMap<>();

        for (DataSetResource sourceDataSetResource : sourceDataSetResources) {
            DataSetResource migratedDataSetResource = tryMigrate(sourceDataSetResource);
            if (migratedDataSetResource != null) {
                migratedDataSetFiles.put(sourceDataSetResource, migratedDataSetResource);
            }
            progressListener.worked(1);
        }

        migrationListener.dataSetCollectionMigrationFinished(migratedDataSetFiles);

        return new DataSetCollectionMigrationResult(migratedDataSetFiles);
    }

    private void checkMigrationPreconditions() {
        if (getDatabaseContainerSupport() == null) {
            throw new IllegalStateException("datasetContainerSupport must be set");
        }
        if (getTargetDataSetFileSupplier() == null) {
            throw new IllegalStateException("targetDataSetFileSupplier must be set");
        }
    }

    protected DataSetResource tryMigrate(DataSetResource sourceDataSetResource) {
        try {
            DataSetResource migratedDataSetFile = migrate(sourceDataSetResource);
            migrationListener.successfullyMigrated(migratedDataSetFile);
            return migratedDataSetFile;
        } catch (DataSetException e) {
            migrationListener.failedMigration(sourceDataSetResource, e);
            return null;
        }
    }


    protected DataSetResource migrate(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetFlywayMigration flywayMigration = createDataSetFlywayMigration(sourceDataSetResource);
        flywayMigration.setDatabaseContainerSupport(getDatabaseContainerSupport());
        flywayMigration.setBeforeMigrationTransformer(getBeforeMigrationTransformer());
        flywayMigration.setAfterMigrationTransformer(getAfterMigrationTransformer());

        TargetDataSetResourceSupplier targetDataSetFileSupplier = getTargetDataSetFileSupplier();
        DataSetResource targetDataSetResource = targetDataSetFileSupplier.getTargetDataSetResource(sourceDataSetResource);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetResource.createConsumer();
        flywayMigration.setDataSetConsumer(targetDataSetFileConsumer);

        migrationListener.startMigration(sourceDataSetResource);

        flywayMigration.exec();

        return targetDataSetResource;
    }


    @NotNull
    protected DataSetFlywayMigration createDataSetFlywayMigration(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
        flywayMigration.setDataSetProducer(sourceDataSetResource.createProducer());
        flywayMigration.apply(this);
        return flywayMigration;
    }

}
