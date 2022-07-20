package com.link_intersystems.dbunit.commands.flyway;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.operation.DatabaseOperation;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer extends DefaultConsumer {

    private JdbcDatabaseContainer<?> jdbcDatabaseContainer;
    private DatabaseConnection databaseConnection;

    private IDataSetConsumer resultConsumer;
    private DatabaseMigrationSupport migrationSupport;
    private Connection connection;
    private AbstractDataSource dataSource;
    private DefaultTable currentTable;

    public void setResultConsumer(IDataSetConsumer resultConsumer) {
        this.resultConsumer = resultConsumer;
    }

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport migrationSupport) {
        this.migrationSupport = migrationSupport;
    }

    @Override
    public void startDataSet() throws DataSetException {
        jdbcDatabaseContainer = createDatabaseContainer();
        jdbcDatabaseContainer.start();

        String jdbcUrl = jdbcDatabaseContainer.getJdbcUrl();
        try {
            String username = jdbcDatabaseContainer.getUsername();
            String password = jdbcDatabaseContainer.getPassword();
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false);

            dataSource = new AbstractDataSource() {

                @Override
                public Connection getConnection() {
                    return ReusableConnectionProxy.createProxy(connection);
                }

                @Override
                public Connection getConnection(String username, String password) {
                    return ReusableConnectionProxy.createProxy(connection);
                }
            };
            databaseConnection = new DatabaseConnection(connection);
        } catch (SQLException | DatabaseUnitException e) {
            throw new DataSetException(e);
        }

        migrationSupport.startDataSet(dataSource);
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
            migrationSupport.endDataSet(dataSource);
            try (Statement statement = connection.createStatement()) {
                statement.execute("commit");
            }

            processResult(databaseConnection, resultConsumer);
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            close(databaseConnection, jdbcDatabaseContainer);
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

    protected void close(DatabaseConnection databaseConnection, JdbcDatabaseContainer<?> jdbcDatabaseContainer) throws DataSetException {
        try {
            databaseConnection.close();
            connection = null;
            dataSource = null;
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            jdbcDatabaseContainer.stop();
        }
    }

    protected JdbcDatabaseContainer<?> createDatabaseContainer() {
        PostgreSQLContainer<?> latest = new PostgreSQLContainer<>("postgres:latest");
        return latest;
    }
}
