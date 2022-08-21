package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.migration.DataSetMigration;
import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetResourcesMigration {

    private DataSetResourcesMigrationListener migrationListener = new LoggingDataSetResourcesMigrationListener();

    private TargetDataSetResourceSupplier targetDataSetResourceSupplier;

    private MigrationDataSetTransformerFactory migrationDataSetTransformerFactory;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;
    private DatabaseMigrationSupport databaseMigrationSupport;

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    public DatabaseMigrationSupport getDatabaseMigrationSupport() {
        return databaseMigrationSupport;
    }

    public void setMigrationListener(DataSetResourcesMigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
    }

    public void setBeforeMigration(DataSetTransormer beforeMigrationTransformer) {
        this.beforeMigrationTransformer = beforeMigrationTransformer;
    }

    public void setBeforeMigration(DataSetConsumerPipe beforeConsumerPipe) {
        this.beforeMigrationTransformer = new DataSetConsumerPipeTransformerAdapter(requireNonNull(beforeConsumerPipe));
    }

    public DataSetTransormer getBeforeMigrationTransformer() {
        return beforeMigrationTransformer;
    }

    public void setAfterMigrationTransformer(DataSetTransormer afterMigrationTransformer) {
        this.afterMigrationTransformer = afterMigrationTransformer;
    }

    public void setAfterMigrationTransformer(DataSetConsumerPipe afterConsumerPipe) {
        this.afterMigrationTransformer =  new DataSetConsumerPipeTransformerAdapter(requireNonNull(afterConsumerPipe));
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

    public void setMigrationDataSetTransformerFactory(MigrationDataSetTransformerFactory migrationDataSetTransformerFactory) {
        this.migrationDataSetTransformerFactory = migrationDataSetTransformerFactory;
    }

    public MigrationDataSetTransformerFactory getMigrationDataSetTransformerFactory() {
        return migrationDataSetTransformerFactory;
    }

    public MigrationsResult exec(List<DataSetResource> sourceDataSetResources) {
        return exec(sourceDataSetResources, NullProgressListener.INSTANCE);
    }

    public MigrationsResult exec(List<DataSetResource> sourceDataSetResources, ProgressListener progressListener) {
        requireNonNull(sourceDataSetResources, "sourceDataSetResources must not be null");
        progressListener = progressListener == null ? NullProgressListener.INSTANCE : progressListener;

        progressListener.begin(sourceDataSetResources.size());
        try {
            return migrate(progressListener, sourceDataSetResources);
        } finally {
            progressListener.done();
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
        if (getMigrationDataSetTransformerFactory() == null) {
            throw new IllegalStateException("migrationDataSetTransformerFactory must be set");
        }
        if (getTargetDataSetResourceSupplier() == null) {
            throw new IllegalStateException("targetDataSetFileSupplier must be set");
        }
        if (getDatabaseMigrationSupport() == null) {
            throw new IllegalStateException("databaseMigrationSupport must be set");
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
        DataSetMigration dataSetMigration = createDataSetFlywayMigration(sourceDataSetResource);
        dataSetMigration.setMigrationDataSetTransformerFactory(getMigrationDataSetTransformerFactory());
        dataSetMigration.setBeforeMigration(getBeforeMigrationTransformer());
        dataSetMigration.setAfterMigration(getAfterMigrationTransformer());

        TargetDataSetResourceSupplier targetDataSetFileSupplier = getTargetDataSetResourceSupplier();
        DataSetResource targetDataSetResource = targetDataSetFileSupplier.getTargetDataSetResource(sourceDataSetResource);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetResource.createConsumer();
        dataSetMigration.setDataSetConsumer(targetDataSetFileConsumer);

        migrationListener.startMigration(sourceDataSetResource);

        dataSetMigration.exec();

        return targetDataSetResource;
    }


    protected DataSetMigration createDataSetFlywayMigration(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetMigration dataSetMigration = new DataSetMigration();
        dataSetMigration.setDatabaseMigrationSupport(getDatabaseMigrationSupport());
        dataSetMigration.setDataSetProducer(sourceDataSetResource.createProducer());
        return dataSetMigration;
    }
}
