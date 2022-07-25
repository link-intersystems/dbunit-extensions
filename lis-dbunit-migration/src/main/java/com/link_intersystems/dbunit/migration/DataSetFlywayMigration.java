package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransformExecutor;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransformerChain;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersDataSetTransformer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFlywayMigration extends AbstractFlywayConfigurationSupport implements DataSetSourceSupport, DataSetConsumerSupport {

    private DataSetSource sourceDataSet;
    private IDataSetConsumer targetConsumer;
    private DatabaseContainerSupport databaseContainerSupport;
    private boolean removeFlywayTables = true;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        targetConsumer = dataSetConsumer;
    }

    @Override
    public void setDataSetSource(DataSetSource dataSetSource) {
        this.sourceDataSet = dataSetSource;
    }

    public void setDatabaseContainerSupport(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = databaseContainerSupport;
    }

    public void setRemoveFlywayTables(boolean removeFlywayTables) {
        this.removeFlywayTables = removeFlywayTables;
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
        DataSetTransformExecutor transformExecutor = new DataSetTransformExecutor();

        IDataSet dataSet = sourceDataSet.get();
        transformExecutor.setDataSetProducer(dataSet);

        transformExecutor.setDataSetConsumer(targetConsumer);

        TestContainersDataSetTransformer migrationTransformer = createMigrationTransformer();

        DataSetTransormer dataSetTransormer = applyBeforeAndAfterTransformers(migrationTransformer);
        transformExecutor.setDataSetTransformer(dataSetTransormer);

        transformExecutor.exec();
    }

    protected TestContainersDataSetTransformer createMigrationTransformer() {
        TestContainersDataSetTransformer transformer = new TestContainersDataSetTransformer(databaseContainerSupport);
        FlywayDatabaseMigrationSupport flywaySupport = new FlywayDatabaseMigrationSupport();
        flywaySupport.setRemoveFlywayTables(removeFlywayTables);
        flywaySupport.apply(this);
        transformer.setDatabaseMigrationSupport(flywaySupport);
        return transformer;
    }

    protected DataSetTransormer applyBeforeAndAfterTransformers(TestContainersDataSetTransformer transformer) {
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
