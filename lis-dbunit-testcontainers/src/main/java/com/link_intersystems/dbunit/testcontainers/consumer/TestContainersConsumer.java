package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.stream.consumer.NullDatabaseMigrationSupport;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.operation.DatabaseOperation;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer extends DefaultConsumer {

    private final DBunitJdbcContainer dBunitJdbcContainer;
    private IDataSetConsumer resultConsumer = new DefaultConsumer();
    private DatabaseMigrationSupport migrationSupport = new NullDatabaseMigrationSupport();
    private DefaultTable currentTable;
    private RunningContainer runningContainer;

    public TestContainersConsumer(DatabaseContainerSupport databaseContainerSupport) {
        this(new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig()));
    }

    public TestContainersConsumer(DBunitJdbcContainer dBunitJdbcContainer) {
        this.dBunitJdbcContainer = Objects.requireNonNull(dBunitJdbcContainer);
    }

    public void setResultConsumer(IDataSetConsumer resultConsumer) {
        this.resultConsumer = requireNonNull(resultConsumer);
    }

    public void setDatabaseMigrationSupport(DatabaseMigrationSupport migrationSupport) {
        this.migrationSupport = requireNonNull(migrationSupport);
    }

    @Override
    public void startDataSet() throws DataSetException {
        runningContainer = dBunitJdbcContainer.start();

        DataSource dataSource = runningContainer.getDataSource();
        migrationSupport.prepareDataSource(dataSource);
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
            IDatabaseConnection databaseConnection = runningContainer.getDatabaseConnection();
            DatabaseOperation.INSERT.execute(databaseConnection, defaultDataSet);
        } catch (SQLException | DatabaseUnitException e) {
            throw new DataSetException(e);
        }

        currentTable = null;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            DataSource dataSource = runningContainer.getDataSource();
            migrationSupport.migrateDataSource(dataSource);

            IDatabaseConnection databaseConnection = runningContainer.getDatabaseConnection();
            processResult(databaseConnection, resultConsumer);
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            runningContainer.stop();
        }
    }

    protected void processResult(IDatabaseConnection databaseConnection, IDataSetConsumer resultConsumer) throws SQLException, DataSetException {
        DatabaseDataSet databaseDataSet = createDataSet(databaseConnection);
        IDataSet decorateResultDataSet = migrationSupport.decorateResultDataSet(databaseConnection, databaseDataSet);
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(decorateResultDataSet);
        dataSetProducerAdapter.setConsumer(resultConsumer);
        dataSetProducerAdapter.produce();
    }

    protected DatabaseDataSet createDataSet(IDatabaseConnection databaseConnection) throws SQLException {
        return new DatabaseDataSet(databaseConnection, false);
    }
}
