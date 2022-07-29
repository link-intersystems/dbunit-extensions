package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.stream.consumer.*;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersDataSetTransformer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFlywayMigration implements DataSetSourceSupport, DataSetConsumerSupport {

    private DataSetSource sourceDataSet;
    private IDataSetConsumer targetConsumer;
    private DatabaseContainerSupport databaseContainerSupport;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;
    private FlywayMigrationConfig migrationConfig = new FlywayMigrationConfig();

    public void setMigrationConfig(FlywayMigrationConfig migrationConfig) {
        this.migrationConfig = requireNonNull(migrationConfig);
    }

    public FlywayMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

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
        if (sourceDataSet == null) {
            throw new IllegalStateException("source dataset must be set");
        }
        if (targetConsumer == null) {
            throw new IllegalStateException("target consumer must be set");
        }

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
        DatabaseMigrationSupport flywaySupport = createFlywayMigrationSupport();

        return new TestContainersDataSetTransformer(databaseContainerSupport, flywaySupport);
    }

    protected FlywayDatabaseMigrationSupport createFlywayMigrationSupport() {
        return new FlywayDatabaseMigrationSupport(getMigrationConfig());
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
