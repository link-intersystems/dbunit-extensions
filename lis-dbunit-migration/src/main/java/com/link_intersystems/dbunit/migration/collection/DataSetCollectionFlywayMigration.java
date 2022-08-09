package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.DataSetFlywayMigration;
import com.link_intersystems.dbunit.migration.resources.TargetDataSetResourceSupplier;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.DataSetResourcesSupplier;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration {

    private DataSetsMigrationListener migrationListener = new LoggingDataSetsMigrationListener();

    private TargetDataSetResourceSupplier targetDataSetResourceSupplier;

    private DataSetResourcesSupplier dataSetResourcesSupplier;
    private DatabaseContainerSupport databaseContainerSupport;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;
    private FlywayMigrationConfig migrationConfig;

    public void setMigrationConfig(FlywayMigrationConfig migrationConfig) {
        this.migrationConfig = migrationConfig;
    }

    public FlywayMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    public void setDataSetResourcesSupplier(DataSetResourcesSupplier dataSetResourcesSupplier) {
        this.dataSetResourcesSupplier = dataSetResourcesSupplier;
    }

    public DataSetResourcesSupplier getDataSetResourcesSupplier() {
        return dataSetResourcesSupplier;
    }

    public void setMigrationListener(DataSetsMigrationListener migrationListener) {
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

    public void setTargetDataSetResourceSupplier(TargetDataSetResourceSupplier targetDataSetResourceSupplier) {
        this.targetDataSetResourceSupplier = requireNonNull(targetDataSetResourceSupplier);
    }

    public TargetDataSetResourceSupplier getTargetDataSetResourceSupplier() {
        return targetDataSetResourceSupplier;
    }

    /**
     * @see DatabaseContainerSupport#getDatabaseContainerSupport(String)
     */
    public void setDatabaseContainerSupport(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = databaseContainerSupport;
    }

    public DatabaseContainerSupport getDatabaseContainerSupport() {
        return databaseContainerSupport;
    }

    public MigrationsResult exec() {
        return exec(NullProgressListener.INSTANCE);
    }

    public MigrationsResult exec(ProgressListener progressListener) {
        DataSetResourcesSupplier dataSetResourcesSupplier = getDataSetResourcesSupplier();
        if (dataSetResourcesSupplier == null) {
            throw new IllegalStateException("dataSetResourcesSupplier must be set");
        }

        List<DataSetResource> sourceDataSetResources = dataSetResourcesSupplier.getDataSetResources();
        migrationListener.resourcesSupplied(sourceDataSetResources);

        boolean proceedMigration = migrationListener.migrationsAboutToStart(sourceDataSetResources);
        if (proceedMigration) {
            progressListener.begin(sourceDataSetResources.size());
            try {
                return migrate(progressListener, sourceDataSetResources);
            } finally {
                progressListener.done();
            }
        } else {
            return new MigrationsResult(new HashMap<>());
        }
    }

    private MigrationsResult migrate(ProgressListener progressListener, List<DataSetResource> sourceDataSetResources) {
        checkMigrationPreconditions();

        Map<DataSetResource, DataSetResource> migratedDataSetFiles = new LinkedHashMap<>();

        for (DataSetResource sourceDataSetResource : sourceDataSetResources) {
            DataSetResource migratedDataSetResource = tryMigrate(sourceDataSetResource);
            if (migratedDataSetResource != null) {
                migratedDataSetFiles.put(sourceDataSetResource, migratedDataSetResource);
            }
            progressListener.worked(1);
        }

        MigrationsResult migrationsResult = new MigrationsResult(migratedDataSetFiles);
        migrationListener.migrationsFinished(migrationsResult);

        return migrationsResult;
    }

    private void checkMigrationPreconditions() {
        if (getDatabaseContainerSupport() == null) {
            throw new IllegalStateException("datasetContainerSupport must be set");
        }
        if (getTargetDataSetResourceSupplier() == null) {
            throw new IllegalStateException("targetDataSetFileSupplier must be set");
        }
        if (getMigrationConfig() == null) {
            throw new IllegalStateException("migrationConfig must be set");
        }
    }

    protected DataSetResource tryMigrate(DataSetResource sourceDataSetResource) {
        try {
            DataSetResource migratedDataSetFile = migrate(sourceDataSetResource);
            migrationListener.migrationSuccessful(migratedDataSetFile);
            return migratedDataSetFile;
        } catch (DataSetException e) {
            migrationListener.migrationFailed(sourceDataSetResource, e);
            return null;
        }
    }


    protected DataSetResource migrate(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetFlywayMigration flywayMigration = createDataSetFlywayMigration(sourceDataSetResource);
        flywayMigration.setDatabaseContainerSupport(getDatabaseContainerSupport());
        flywayMigration.setBeforeMigrationTransformer(getBeforeMigrationTransformer());
        flywayMigration.setAfterMigrationTransformer(getAfterMigrationTransformer());

        TargetDataSetResourceSupplier targetDataSetFileSupplier = getTargetDataSetResourceSupplier();
        DataSetResource targetDataSetResource = targetDataSetFileSupplier.getTargetDataSetResource(sourceDataSetResource);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetResource.createConsumer();
        flywayMigration.setDataSetConsumer(targetDataSetFileConsumer);

        migrationListener.startMigration(sourceDataSetResource);

        flywayMigration.exec();

        return targetDataSetResource;
    }


    protected DataSetFlywayMigration createDataSetFlywayMigration(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
        flywayMigration.setMigrationConfig(getMigrationConfig());
        flywayMigration.setDataSetProducer(sourceDataSetResource.createProducer());
        return flywayMigration;
    }
}
