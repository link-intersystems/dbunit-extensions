package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.migration.DataSetMigration;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetResourcesMigration {

    private DataSetResourcesMigrationListener migrationListener = new LoggingDataSetResourcesMigrationListener();

    private TargetDataSetResourceSupplier targetDataSetResourceSupplier;

    private MigrationDataSetTransformerFactory migrationDataSetTransformerFactory;
    private Supplier<DataSetTransormer> beforeMigrationTransformerSupplier = () -> null;
    private Supplier<DataSetTransormer> afterMigrationTransformerSupplier = () -> null;
    private DatabaseMigrationSupport databaseMigrationSupport;

    private ExecutorService executorService = Executors.newFixedThreadPool(4);

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    public DatabaseMigrationSupport getDatabaseMigrationSupport() {
        return databaseMigrationSupport;
    }

    public void setMigrationListener(DataSetResourcesMigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
    }

    public void setBeforeMigrationSupplier(Supplier<DataSetTransormer> beforeMigrationTransformerSupplier) {
        this.beforeMigrationTransformerSupplier = requireNonNull(beforeMigrationTransformerSupplier);
    }

    public void setAfterMigrationSupplier(Supplier<DataSetTransormer> afterMigrationTransformerSupplier) {
        this.afterMigrationTransformerSupplier = requireNonNull(afterMigrationTransformerSupplier);
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

        List<Future<DataSetResource>> migrationFutures = new ArrayList<>();
        Map<Future<DataSetResource>, DataSetResource> sourceDataSetResourcesByMigration = new HashMap<>();


        for (DataSetResource sourceDataSetResource : sourceDataSetResources) {
            Future<DataSetResource> migrationFuture = executorService.submit(() -> tryMigrate(sourceDataSetResource));
            migrationFutures.add(migrationFuture);
            sourceDataSetResourcesByMigration.put(migrationFuture, sourceDataSetResource);

            progressListener.worked(1);
        }

        for (Future<DataSetResource> migrationFuture : migrationFutures) {
            try {
                DataSetResource migratedDataSetResource = migrationFuture.get();
                if (migratedDataSetResource != null) {
                    DataSetResource sourceDataSetResource = sourceDataSetResourcesByMigration.get(migrationFuture);
                    migratedDataSetFiles.put(sourceDataSetResource, migratedDataSetResource);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
            }

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
        dataSetMigration.setBeforeMigration(beforeMigrationTransformerSupplier.get());
        dataSetMigration.setAfterMigration(afterMigrationTransformerSupplier.get());

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
