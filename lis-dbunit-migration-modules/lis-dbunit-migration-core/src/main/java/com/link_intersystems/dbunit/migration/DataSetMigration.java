package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.*;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetMigration implements DataSetSourceSupport, DataSetConsumerSupport {

    private DataSetSource sourceDataSet;
    private IDataSetConsumer targetConsumer;
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

    public void setMigrationDataSetTransformerFactory(MigrationDataSetTransformerFactory migrationDataSetTransformerFactory) {
        this.migrationDataSetTransformerFactory = migrationDataSetTransformerFactory;
    }

    public MigrationDataSetTransformerFactory getMigrationDataSetTransformerFactory() {
        return migrationDataSetTransformerFactory;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        targetConsumer = dataSetConsumer;
    }

    @Override
    public void setDataSetSource(DataSetSource dataSetSource) {
        this.sourceDataSet = dataSetSource;
    }

    public void setBeforeMigrationTransformer(DataSetTransormer beforeMigrationTransformer) {
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

    public void exec() throws DataSetException {
        checkConfiguredProperly();

        DataSetTransformExecutor transformExecutor = new DataSetTransformExecutor();

        IDataSet dataSet = sourceDataSet.get();
        transformExecutor.setDataSetProducer(dataSet);

        transformExecutor.setDataSetConsumer(targetConsumer);

        MigrationDataSetTransformerFactory transformerFactory = getMigrationDataSetTransformerFactory();
        DataSetTransormer migrationTransformer = transformerFactory.createTransformer(getDatabaseMigrationSupport());

        DataSetTransormer dataSetTransormer = applyBeforeAndAfterTransformers(migrationTransformer);
        transformExecutor.setDataSetTransformer(dataSetTransormer);

        transformExecutor.exec();
    }

    private void checkConfiguredProperly() {
        if (sourceDataSet == null) {
            throw new IllegalStateException("source dataset must be set");
        }
        if (targetConsumer == null) {
            throw new IllegalStateException("target consumer must be set");
        }
        if(getDatabaseMigrationSupport() == null){
            throw new IllegalStateException("databaseMigrationSupport must be set");
        }
        if(getMigrationDataSetTransformerFactory() == null){
            throw new IllegalStateException("migrationDataSetTransformerFactory must be set");
        }
    }

    protected DataSetTransormer applyBeforeAndAfterTransformers(DataSetTransormer transformer) {
        DataSetTransformerChain dataSetTransformerChain = new DataSetTransformerChain();

        if (getBeforeMigrationTransformer() != null) {
            dataSetTransformerChain.add(getBeforeMigrationTransformer());
        }

        dataSetTransformerChain.add(transformer);

        if (getAfterMigrationTransformer() != null) {
            dataSetTransformerChain.add(getAfterMigrationTransformer());
        }

        return dataSetTransformerChain;
    }


}
