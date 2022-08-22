package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
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
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer implements DataSetConsumerPipe {

    private DatabaseOperation databaseOperation = DatabaseOperation.INSERT;
    private IDataSetConsumer dataSetConsumer = new DefaultConsumer();
    private DataSourceConsumer startDataSourceConsumer = NullDataSourceConsumer.INSTANCE;
    private DataSourceConsumer endDataSourceConsumer = NullDataSourceConsumer.INSTANCE;
    private DefaultTable rowCache;
    private RunningContainer runningContainer;

    private RunningContainerPool runningContainerPool;

    private int rowCacheLimit = Integer.MAX_VALUE;

    public TestContainersConsumer(DatabaseContainerSupport databaseContainerSupport) {
        Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier = () -> new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig());
        this.runningContainerPool = new SingleRunningContainerPool(
                dBunitJdbcContainerSupplier
        );
    }

    public TestContainersConsumer(RunningContainerPool runningContainerPool) {
        this.runningContainerPool = Objects.requireNonNull(runningContainerPool);
    }

    public void setDatabaseOperation(DatabaseOperation databaseOperation) {
        this.databaseOperation = requireNonNull(databaseOperation);
    }

    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
    }

    @Override
    public void setSubsequentConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = requireNonNull(dataSetConsumer);
    }

    /**
     * The rowCacheLimit property controls when the cached table rows are passed to
     * the test containers {@link DataSource} using the {@link #setDatabaseOperation(DatabaseOperation)}.
     * When a table starts ({@link #startTable(ITableMetaData)}) an internal cache is created that caches the
     * table rows on every {@link #row(Object[])} call. If the row count of the cache exceeds the rowCacheLimit
     * the rows are passed to the {@link DataSource} of the test container.
     * If you  want to turn it off you should set the rowCacheLimit to {@link Integer#MAX_VALUE} with will virtually turn
     * it off since the table row cache can at most contain {@link Integer#MAX_VALUE} rows.
     * {@link Integer#MAX_VALUE} is the default.
     *
     * @param rowCacheLimit
     */
    public void setRowCacheLimit(int rowCacheLimit) {
        if (rowCacheLimit < 1) {
            throw new IllegalArgumentException("rowCacheLimit must be 1 or greater");
        }
        this.rowCacheLimit = rowCacheLimit;
    }

    /**
     * A {@link DataSourceConsumer} that will be invoked with the test containers {@link DataSource} on {@link #startDataSet()} .
     *
     * @param startDataSourceConsumer
     */
    public void setStartDataSourceConsumer(DataSourceConsumer startDataSourceConsumer) {
        this.startDataSourceConsumer = requireNonNull(startDataSourceConsumer);
    }

    /**
     * A {@link DataSourceConsumer} that will be invoked with the test containers {@link DataSource} on {@link #endDataSet()}.
     *
     * @param endDataSourceConsumer
     */
    public void setEndDataSourceConsumer(DataSourceConsumer endDataSourceConsumer) {
        this.endDataSourceConsumer = requireNonNull(endDataSourceConsumer);
    }

    @Override
    public void startDataSet() throws DataSetException {
        runningContainer = runningContainerPool.borrowContainer();

        DataSource dataSource = runningContainer.getDataSource();
        try {
            startDataSourceConsumer.consume(dataSource);
        } catch (SQLException e) {
            throw new DataSetException("StartDataSetConsumer threw exception.", e);
        }
    }


    @Override
    public void startTable(ITableMetaData metaData) {
        rowCache = new DefaultTable(metaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        rowCache.addRow(values);
        if (isRowCacheLimitExceeded()) {
            flushRowCache();
            rowCache = new DefaultTable(rowCache.getTableMetaData());
        }
    }

    protected boolean isRowCacheLimitExceeded() {
        return rowCache != null && rowCache.getRowCount() >= rowCacheLimit;
    }

    protected void flushRowCache() throws DataSetException {
        if (rowCache == null || rowCache.getRowCount() == 0) {
            return;
        }

        IDatabaseConnection databaseConnection = runningContainer.getDatabaseConnection();
        DatabaseOperation databaseOperation = getDatabaseOperation();
        processTable(databaseOperation, databaseConnection, rowCache);
    }

    @Override
    public void endTable() throws DataSetException {
        flushRowCache();

        rowCache = null;
    }

    protected void processTable(DatabaseOperation operation, IDatabaseConnection connection, ITable table) throws DataSetException {
        try {
            DefaultDataSet tmpDataSet = new DefaultDataSet();
            tmpDataSet.addTable(table);

            operation.execute(connection, tmpDataSet);
        } catch (SQLException | DatabaseUnitException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            DataSource dataSource = runningContainer.getDataSource();
            endDataSourceConsumer.consume(dataSource);

            IDatabaseConnection databaseConnection = runningContainer.getDatabaseConnection();
            processResult(databaseConnection, dataSetConsumer);
        } catch (SQLException e) {
            throw new DataSetException(e);
        } finally {
            runningContainerPool.returnContainer(runningContainer);
        }
    }

    protected void processResult(IDatabaseConnection databaseConnection, IDataSetConsumer resultConsumer) throws SQLException, DataSetException {
        DatabaseDataSet databaseDataSet = createDataSet(databaseConnection);
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(databaseDataSet);
        dataSetProducerAdapter.setConsumer(resultConsumer);
        dataSetProducerAdapter.produce();
    }

    protected DatabaseDataSet createDataSet(IDatabaseConnection databaseConnection) throws SQLException {
        return new DatabaseDataSet(databaseConnection, false);
    }
}
