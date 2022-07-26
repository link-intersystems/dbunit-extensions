package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.consumer.NullDatabaseMigrationSupport;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.operation.DatabaseOperation;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.SQLException;

import static java.util.Objects.requireNonNull;
import static org.dbunit.database.DatabaseConfig.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer extends DefaultConsumer {

    private JdbcDatabaseContainer<?> jdbcDatabaseContainer;
    private DatabaseConnection databaseConnection;

    private IDataSetConsumer resultConsumer = new DefaultConsumer();
    private DatabaseMigrationSupport migrationSupport = new NullDatabaseMigrationSupport();
    private DefaultTable currentTable;
    private DatabaseContainerSupport databaseContainerSupport;
    private DatabaseContainerDataSource dataSource;

    public TestContainersConsumer(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = requireNonNull(databaseContainerSupport);
    }

    public void setResultConsumer(IDataSetConsumer resultConsumer) {
        this.resultConsumer = requireNonNull(resultConsumer);
    }

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport migrationSupport) {
        this.migrationSupport = requireNonNull(migrationSupport);
    }

    @Override
    public void startDataSet() throws DataSetException {
        jdbcDatabaseContainer = databaseContainerSupport.create();
        jdbcDatabaseContainer.start();

        dataSource = new DatabaseContainerDataSource(jdbcDatabaseContainer);
        try {
            databaseConnection = new DatabaseConnection(dataSource.getConnection());
            DatabaseConfig databaseConfig = databaseConnection.getConfig();
            DatabaseConfig containerSupportConfig = databaseContainerSupport.getDatabaseConfig();
            applyContainerSupportConfig(containerSupportConfig, databaseConfig);
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }

        migrationSupport.prepareDataSource(this.dataSource);
    }

    private void applyContainerSupportConfig(DatabaseConfig containerSupportConfig, DatabaseConfig databaseConfig) {
        databaseConfig.setFeature(FEATURE_BATCHED_STATEMENTS, containerSupportConfig.getFeature(FEATURE_BATCHED_STATEMENTS));
        databaseConfig.setFeature(FEATURE_QUALIFIED_TABLE_NAMES, containerSupportConfig.getFeature(FEATURE_QUALIFIED_TABLE_NAMES));
        databaseConfig.setFeature(FEATURE_CASE_SENSITIVE_TABLE_NAMES, containerSupportConfig.getFeature(FEATURE_CASE_SENSITIVE_TABLE_NAMES));
        databaseConfig.setFeature(FEATURE_DATATYPE_WARNING, containerSupportConfig.getFeature(FEATURE_DATATYPE_WARNING));
        databaseConfig.setFeature(FEATURE_ALLOW_EMPTY_FIELDS, containerSupportConfig.getFeature(FEATURE_ALLOW_EMPTY_FIELDS));

        databaseConfig.setProperty(PROPERTY_STATEMENT_FACTORY, containerSupportConfig.getProperty(PROPERTY_STATEMENT_FACTORY));
        databaseConfig.setProperty(PROPERTY_RESULTSET_TABLE_FACTORY, containerSupportConfig.getProperty(PROPERTY_RESULTSET_TABLE_FACTORY));
        databaseConfig.setProperty(PROPERTY_DATATYPE_FACTORY, containerSupportConfig.getProperty(PROPERTY_DATATYPE_FACTORY));
        databaseConfig.setProperty(PROPERTY_ESCAPE_PATTERN, containerSupportConfig.getProperty(PROPERTY_ESCAPE_PATTERN));
        databaseConfig.setProperty(PROPERTY_TABLE_TYPE, containerSupportConfig.getProperty(PROPERTY_TABLE_TYPE));
        databaseConfig.setProperty(PROPERTY_BATCH_SIZE, containerSupportConfig.getProperty(PROPERTY_BATCH_SIZE));
        databaseConfig.setProperty(PROPERTY_FETCH_SIZE, containerSupportConfig.getProperty(PROPERTY_FETCH_SIZE));
        databaseConfig.setProperty(PROPERTY_METADATA_HANDLER, containerSupportConfig.getProperty(PROPERTY_METADATA_HANDLER));
        databaseConfig.setProperty(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH,
                containerSupportConfig.getProperty(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH));
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        currentTable = new DefaultTable(metaData);
        super.startTable(metaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        currentTable.addRow(values);
    }

    @Override
    public void endTable() throws DataSetException {
        DefaultDataSet defaultDataSet = new DefaultDataSet();
        defaultDataSet.addTable(currentTable);
        try {
            DatabaseOperation.INSERT.execute(databaseConnection, defaultDataSet);
        } catch (SQLException | DatabaseUnitException e) {
            throw new DataSetException(e);
        }

        currentTable = null;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            migrationSupport.migrateDataSource(dataSource);

            processResult(databaseConnection, resultConsumer);
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            close(dataSource, databaseConnection, jdbcDatabaseContainer);
        }
    }

    protected void processResult(DatabaseConnection databaseConnection, IDataSetConsumer resultConsumer) throws SQLException, DataSetException {
        DatabaseDataSet databaseDataSet = createDataSet(databaseConnection);
        IDataSet decorateResultDataSet = migrationSupport.decorateResultDataSet(databaseConnection, databaseDataSet);
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(decorateResultDataSet);
        dataSetProducerAdapter.setConsumer(resultConsumer);
        dataSetProducerAdapter.produce();
    }

    protected DatabaseDataSet createDataSet(DatabaseConnection databaseConnection) throws SQLException {
        return new DatabaseDataSet(databaseConnection, false);
    }

    protected void close(DatabaseContainerDataSource dataSource, DatabaseConnection databaseConnection, JdbcDatabaseContainer<?> jdbcDatabaseContainer) throws DataSetException {
        try {
            databaseConnection.close();
            this.dataSource = null;
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            try {
                dataSource.close();
            } finally {
                stopContainer(jdbcDatabaseContainer);
            }
        }
    }

    protected void stopContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer) {
        jdbcDatabaseContainer.stop();
    }

}
