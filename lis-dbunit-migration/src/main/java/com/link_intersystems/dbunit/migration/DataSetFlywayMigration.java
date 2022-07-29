package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.DefaultFlywayMigration;
import com.link_intersystems.dbunit.flyway.FlywayDataSetMigrationConfig;
import com.link_intersystems.dbunit.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransformExecutor;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransformerChain;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersDataSetTransformer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFlywayMigration implements DataSetSourceSupport, DataSetConsumerSupport {

    private DataSetSource sourceDataSet;
    private IDataSetConsumer targetConsumer;
    private DatabaseContainerSupport databaseContainerSupport;
    private boolean removeFlywayTables = true;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;
    private FlywayDataSetMigrationConfig migrationConfig = new FlywayDataSetMigrationConfig();
    private FluentConfiguration flywayConfiguration = Flyway.configure();

    public void setFlywayConfiguration(FluentConfiguration flywayConfiguration) {
        this.flywayConfiguration = requireNonNull(flywayConfiguration);
    }

    public FluentConfiguration getFlywayConfiguration() {
        return flywayConfiguration;
    }

    public void setMigrationConfig(FlywayDataSetMigrationConfig migrationConfig) {
        this.migrationConfig = requireNonNull(migrationConfig);
    }

    public FlywayDataSetMigrationConfig getMigrationConfig() {
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
        TestContainersDataSetTransformer transformer = new TestContainersDataSetTransformer(databaseContainerSupport);
        FlywayDatabaseMigrationSupport flywaySupport = new FlywayDatabaseMigrationSupport();
        flywaySupport.setMigrationConfig(getMigrationConfig());
        DefaultFlywayMigration flywayMigration = new DefaultFlywayMigration();
        flywayMigration.setFlywayConfigurationSupplier(this::getFlywayConfigurationCopy);
        flywaySupport.setFlywayMigration(flywayMigration);
        flywaySupport.setRemoveFlywayTables(removeFlywayTables);
        transformer.setDatabaseMigrationSupport(flywaySupport);
        return transformer;
    }

    protected FluentConfiguration getFlywayConfigurationCopy() {
        FluentConfiguration configure = Flyway.configure();

        configure.placeholders(getFlywayConfiguration().getPlaceholders());
        configure.locations(getFlywayConfiguration().getLocations());
        configure.javaMigrations(getFlywayConfiguration().getJavaMigrations());
        configure.javaMigrationClassProvider(getFlywayConfiguration().getJavaMigrationClassProvider());
        configure.callbacks(getFlywayConfiguration().getCallbacks());
        configure.encoding(getFlywayConfiguration().getEncoding());
        configure.defaultSchema(getFlywayConfiguration().getDefaultSchema());

        return configure;
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
