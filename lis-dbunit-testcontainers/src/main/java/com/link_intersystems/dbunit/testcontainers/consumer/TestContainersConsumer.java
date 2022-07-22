package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.operation.DatabaseOperation;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer extends DefaultConsumer {

    private JdbcDatabaseContainer<?> jdbcDatabaseContainer;
    private DatabaseConnection databaseConnection;

    private IDataSetConsumer resultConsumer;
    private DatabaseMigrationSupport migrationSupport;
    private DefaultTable currentTable;
    private DatabaseContainerFactory databaseContainerFactory;
    private DatabaseContainerDataSource dataSource;

    private List<String> consumedTableNames;

    public TestContainersConsumer(DatabaseContainerFactory databaseContainerFactory) {
        this.databaseContainerFactory = requireNonNull(databaseContainerFactory);
    }

    public void setResultConsumer(IDataSetConsumer resultConsumer) {
        this.resultConsumer = resultConsumer;
    }

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport migrationSupport) {
        this.migrationSupport = migrationSupport;
    }

    @Override
    public void startDataSet() throws DataSetException {
        jdbcDatabaseContainer = databaseContainerFactory.create();
        jdbcDatabaseContainer.start();

        dataSource = new DatabaseContainerDataSource(jdbcDatabaseContainer);
        try {
            databaseConnection = new DatabaseConnection(dataSource.getConnection());
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }

        migrationSupport.prepareDataSource(this.dataSource);
        consumedTableNames = new ArrayList<>();
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        currentTable = new DefaultTable(metaData);
        super.startTable(metaData);

        consumedTableNames.add(metaData.getTableName());
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
