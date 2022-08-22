package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.*;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetMigration implements DataSetProducerSupport, DataSetConsumerSupport {

    private IDataSetProducer sourceProducer;
    private IDataSetConsumer targetConsumer;
    private MigrationDataSetTransformerFactory migrationDataSetTransformerFactory;
    private DataSetTransormer beforeMigration;
    private DataSetTransormer afterMigration;
    private DatabaseMigrationSupport databaseMigrationSupport;

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    public DatabaseMigrationSupport getDatabaseMigrationSupport() {
        return databaseMigrationSupport;
    }

    public void setMigrationDataSetTransformerFactory(MigrationDataSetTransformerFactory migrationDataSetTransformerFactory) {
        this.migrationDataSetTransformerFactory = migrationDataSetTransformerFactory;
    }

    public MigrationDataSetTransformerFactory getMigrationDataSetTransformerFactory() {
        return migrationDataSetTransformerFactory;
    }

    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        sourceProducer = dataSetProducer;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        targetConsumer = dataSetConsumer;
    }


    public void setBeforeMigration(DataSetTransormer beforeMigrationTransformer) {
        this.beforeMigration = beforeMigrationTransformer;
    }

    public void setBeforeMigration(DataSetConsumerPipe beforeConsumerPipe) {
        this.beforeMigration = new DataSetConsumerPipeTransformerAdapter(requireNonNull(beforeConsumerPipe));
    }

    public void setAfterMigration(DataSetTransormer afterMigrationTransformer) {
        this.afterMigration = afterMigrationTransformer;
    }

    public void setAfterMigration(DataSetConsumerPipe afterConsumerPipe) {
        this.afterMigration = new DataSetConsumerPipeTransformerAdapter(requireNonNull(afterConsumerPipe));
    }

    public DataSetTransormer getBeforeTransformer() {
        return beforeMigration;
    }

    public DataSetTransormer getAfterMigration() {
        return afterMigration;
    }

    public void exec() throws DataSetException {
        checkConfiguredProperly();

        DataSetTransformExecutor transformExecutor = new DataSetTransformExecutor();

        transformExecutor.setDataSetProducer(sourceProducer);

        transformExecutor.setDataSetConsumer(targetConsumer);

        MigrationDataSetTransformerFactory transformerFactory = getMigrationDataSetTransformerFactory();
        DataSetTransormer migrationTransformer = transformerFactory.createTransformer(getDatabaseMigrationSupport());

        DataSetTransormer dataSetTransormer = applyBeforeAndAfterTransformers(migrationTransformer);
        transformExecutor.setDataSetTransformer(dataSetTransormer);

        transformExecutor.exec();
    }

    private void checkConfiguredProperly() {
        if (sourceProducer == null) {
            throw new IllegalStateException("source producer must be set");
        }
        if (targetConsumer == null) {
            throw new IllegalStateException("target consumer must be set");
        }
        if (getDatabaseMigrationSupport() == null) {
            throw new IllegalStateException("databaseMigrationSupport must be set");
        }
        if (getMigrationDataSetTransformerFactory() == null) {
            throw new IllegalStateException("migrationDataSetTransformerFactory must be set");
        }
    }

    protected DataSetTransormer applyBeforeAndAfterTransformers(DataSetTransormer transformer) {
        DataSetTransformerChain dataSetTransformerChain = new DataSetTransformerChain();

        dataSetTransformerChain.add(getBeforeTransformer());
        dataSetTransformerChain.add(transformer);
        dataSetTransformerChain.add(getAfterMigration());

        return dataSetTransformerChain;
    }


}
