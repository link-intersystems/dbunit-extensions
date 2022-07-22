package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransformExecutor;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.JdbcDatabaseContainerFactory;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersDataSetTransformer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.flywaydb.core.api.MigrationVersion;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFlywayMigration extends AbstractFlywayConfigurationSupport implements DataSetSourceSupport, DataSetConsumerSupport {


    private DataSetSource sourceDataSet;
    private IDataSetConsumer targetConsumer;
    private JdbcDatabaseContainerFactory databaseContainerFactory;
    private MigrationVersion sourceVersion;
    private MigrationVersion targetVersion;

    public void setSourceVersion(String sourceVersion) {
        setSourceVersion(MigrationVersion.fromVersion(sourceVersion));
    }

    public void setSourceVersion(MigrationVersion sourceVersion) {
        this.sourceVersion = sourceVersion;
    }


    public void setTargetVersion(String targetVersion) {
        setSourceVersion(MigrationVersion.fromVersion(targetVersion));
    }

    public void setTargetVersion(MigrationVersion targetVersion) {
        this.targetVersion = targetVersion;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        targetConsumer = dataSetConsumer;
    }

    @Override
    public void setDataSetSource(DataSetSource dataSetSource) {
        this.sourceDataSet = dataSetSource;
    }

    public void setJdbcDatabaseContainerFactory(JdbcDatabaseContainerFactory databaseContainerFactory) {
        this.databaseContainerFactory = databaseContainerFactory;
    }

    public void exec() throws DataSetException {
        DataSetTransformExecutor dataSetTransformCommand = new DataSetTransformExecutor();

        IDataSet dataSet = sourceDataSet.get();
        dataSetTransformCommand.setDataSetProducer(dataSet);

        dataSetTransformCommand.setDataSetConsumer(targetConsumer);


        TestContainersDataSetTransformer transformer = new TestContainersDataSetTransformer(databaseContainerFactory);
        FlywayDatabaseMigrationSupport flywaySupport = new FlywayDatabaseMigrationSupport();

        flywaySupport.setRemoveFlywayTables(false);
        flywaySupport.apply(this);
        flywaySupport.setStartVersion(sourceVersion);
        flywaySupport.setEndVersion(targetVersion);
        transformer.setDatabaseMigrationSupport(flywaySupport);
        dataSetTransformCommand.setDataSetTransformer(transformer);

        dataSetTransformCommand.exec();
    }


}
