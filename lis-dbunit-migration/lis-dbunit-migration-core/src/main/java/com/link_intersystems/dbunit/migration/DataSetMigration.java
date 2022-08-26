package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetMigration implements DataSetProducerSupport, DataSetConsumerSupport {

    private IDataSetProducer sourceProducer;
    private IDataSetConsumer targetConsumer;
    private MigrationDataSetPipeFactory migrationDataSetTransformerFactory;
    private ChainableDataSetConsumer beforeMigration;
    private ChainableDataSetConsumer afterMigration;
    private DatabaseMigrationSupport databaseMigrationSupport;

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    public DatabaseMigrationSupport getDatabaseMigrationSupport() {
        return databaseMigrationSupport;
    }

    public void setMigrationDataSetTransformerFactory(MigrationDataSetPipeFactory migrationDataSetTransformerFactory) {
        this.migrationDataSetTransformerFactory = migrationDataSetTransformerFactory;
    }

    public MigrationDataSetPipeFactory getMigrationDataSetTransformerFactory() {
        return migrationDataSetTransformerFactory;
    }

    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        sourceProducer = dataSetProducer;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        targetConsumer = dataSetConsumer;
    }


    public void setBeforeMigration(ChainableDataSetConsumer beforeMigrationTransformer) {
        this.beforeMigration = beforeMigrationTransformer;
    }

    public void setAfterMigration(ChainableDataSetConsumer afterMigrationTransformer) {
        this.afterMigration = afterMigrationTransformer;
    }

    public ChainableDataSetConsumer getBeforeTransformer() {
        return beforeMigration;
    }

    public ChainableDataSetConsumer getAfterMigration() {
        return afterMigration;
    }

    public void exec() throws DataSetException {
        checkProperlyConfigured();

        DataSetConsumerPipe migrationPipe = createMigrationPipe();
        migrationPipe.setOutputConsumer(targetConsumer);

        migrationPipe.execute(sourceProducer);
    }

    private void checkProperlyConfigured() {
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

    protected DataSetConsumerPipe createMigrationPipe() {
        DataSetConsumerPipe migrationProcessPipe = new DataSetConsumerPipe();

        migrationProcessPipe.add(getBeforeTransformer());

        MigrationDataSetPipeFactory pipeFactory = getMigrationDataSetTransformerFactory();
        DataSetConsumerPipe migrationPipe = pipeFactory.createMigrationPipe(getDatabaseMigrationSupport());
        migrationPipe.add(migrationPipe);

        migrationPipe.add(getAfterMigration());

        return migrationPipe;
    }


}
