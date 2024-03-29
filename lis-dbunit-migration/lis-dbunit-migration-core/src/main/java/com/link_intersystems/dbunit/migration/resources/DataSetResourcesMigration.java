package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.migration.DataSetMigration;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetPipeFactory;
import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.util.concurrent.ProgressListener;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetResourcesMigration {

    private MigrationListener migrationListener = new Slf4jLoggingMigrationListener();
    private Logger logger = LoggerFactory.getLogger(DataSetResourcesMigration.class);

    private TargetDataSetResourceSupplier targetDataSetResourceSupplier;
    private MigrationDataSetPipeFactory migrationDataSetTransformerFactory;
    private Supplier<ChainableDataSetConsumer> beforeMigrationConsumerSupplier = () -> null;
    private Supplier<ChainableDataSetConsumer> afterMigrationConsumerSupplier = () -> null;
    private DatabaseMigrationSupport databaseMigrationSupport;
    private ExecutorService executorService;

    public void setLogger(Logger logger) {
        this.logger = requireNonNull(logger);
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    public DatabaseMigrationSupport getDatabaseMigrationSupport() {
        return databaseMigrationSupport;
    }

    public void setMigrationListener(MigrationListener migrationListener) {
        this.migrationListener = requireNonNull(migrationListener);
    }

    public void setBeforeMigrationSupplier(Supplier<ChainableDataSetConsumer> beforeMigrationConsumerSupplier) {
        this.beforeMigrationConsumerSupplier = requireNonNull(beforeMigrationConsumerSupplier);
    }

    public void setAfterMigrationSupplier(Supplier<ChainableDataSetConsumer> afterMigrationConsumerSupplier) {
        this.afterMigrationConsumerSupplier = requireNonNull(afterMigrationConsumerSupplier);
    }

    public void setTargetDataSetResourceSupplier(TargetDataSetResourceSupplier targetDataSetResourceSupplier) {
        this.targetDataSetResourceSupplier = requireNonNull(targetDataSetResourceSupplier);
    }

    public TargetDataSetResourceSupplier getTargetDataSetResourceSupplier() {
        return targetDataSetResourceSupplier;
    }

    public void setMigrationDataSetTransformerFactory(MigrationDataSetPipeFactory migrationDataSetTransformerFactory) {
        this.migrationDataSetTransformerFactory = migrationDataSetTransformerFactory;
    }

    public MigrationDataSetPipeFactory getMigrationDataSetTransformerFactory() {
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

        ExecutorService effectiveExecutorService = executorService;

        if (effectiveExecutorService == null) {
            effectiveExecutorService = getFallbackExecutorService(sourceDataSetResources.size());
        }

        Map<DataSetResource, DataSetResource> migratedDataSetFiles = new LinkedHashMap<>();

        List<Future<DataSetResource>> migrationFutures = new ArrayList<>();
        Map<Future<DataSetResource>, DataSetResource> sourceDataSetResourcesByMigration = new HashMap<>();


        for (DataSetResource sourceDataSetResource : sourceDataSetResources) {
            Future<DataSetResource> migrationFuture = effectiveExecutorService.submit(() -> tryMigrate(sourceDataSetResource, progressListener));
            migrationFutures.add(migrationFuture);
            sourceDataSetResourcesByMigration.put(migrationFuture, sourceDataSetResource);
        }

        for (Future<DataSetResource> migrationFuture : migrationFutures) {
            DataSetResource sourceDataSetResource = sourceDataSetResourcesByMigration.get(migrationFuture);

            try {
                DataSetResource migratedDataSetResource = migrationFuture.get();

                if (migratedDataSetResource != null) {
                    migratedDataSetFiles.put(sourceDataSetResource, migratedDataSetResource);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                logger.error("Unable to migrate {}", sourceDataSetResource, e.getCause());
            }

        }

        MigrationsResult migrationsResult = new MigrationsResult(migratedDataSetFiles);
        migrationListener.migrationsFinished(migrationsResult);

        return migrationsResult;
    }

    protected ExecutorService getFallbackExecutorService(int dataSetResourcesCount) {
        int threadCount = min(dataSetResourcesCount, 5);
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                return new Thread(r, "data-set-migration-thread-" + threadNumber.getAndIncrement());
            }
        };

        return Executors.newFixedThreadPool(threadCount, threadFactory);
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

    protected DataSetResource tryMigrate(DataSetResource sourceDataSetResource, ProgressListener progressListener) {
        try {
            fireStartMigration(sourceDataSetResource);
            DataSetResource migratedDataSetFile = migrate(sourceDataSetResource);
            fireMigrationSuccessful(migratedDataSetFile);
            return migratedDataSetFile;
        } catch (DataSetException e) {
            fireMigrationFailed(sourceDataSetResource, e);
            return null;
        } finally {
            progressWorked(progressListener);
        }
    }

    private void fireStartMigration(DataSetResource sourceDataSetResource) {
        synchronized (migrationListener) {
            migrationListener.startMigration(sourceDataSetResource);
        }
    }

    private void fireMigrationSuccessful(DataSetResource migratedDataSetFile) {
        synchronized (migrationListener) {
            migrationListener.migrationSuccessful(migratedDataSetFile);
        }
    }

    private void fireMigrationFailed(DataSetResource sourceDataSetResource, DataSetException e) {
        synchronized (migrationListener) {
            migrationListener.migrationFailed(sourceDataSetResource, e);
        }
    }

    private void progressWorked(ProgressListener progressListener) {
        synchronized (progressListener) {
            progressListener.worked(1);
        }
    }


    protected DataSetResource migrate(DataSetResource sourceDataSetResource) throws DataSetException {
        DataSetMigration dataSetMigration = createDataSetFlywayMigration(sourceDataSetResource);
        dataSetMigration.setMigrationDataSetTransformerFactory(getMigrationDataSetTransformerFactory());

        ChainableDataSetConsumer beforeMigrationConsumer = beforeMigrationConsumerSupplier.get();
        dataSetMigration.setBeforeMigration(beforeMigrationConsumer);

        ChainableDataSetConsumer afterMigrationConsumer = afterMigrationConsumerSupplier.get();
        dataSetMigration.setAfterMigration(afterMigrationConsumer);

        TargetDataSetResourceSupplier targetDataSetFileSupplier = getTargetDataSetResourceSupplier();
        DataSetResource targetDataSetResource = targetDataSetFileSupplier.getTargetDataSetResource(sourceDataSetResource);
        IDataSetConsumer targetDataSetFileConsumer = targetDataSetResource.createConsumer();
        dataSetMigration.setDataSetConsumer(targetDataSetFileConsumer);

        dataSetMigration.exec();

        return targetDataSetResource;
    }


    protected DataSetMigration createDataSetFlywayMigration(DataSetResource sourceDataSetResource) throws
            DataSetException {
        DataSetMigration dataSetMigration = new DataSetMigration();
        dataSetMigration.setDatabaseMigrationSupport(getDatabaseMigrationSupport());
        dataSetMigration.setDataSetProducer(sourceDataSetResource.createProducer());
        return dataSetMigration;
    }
}
