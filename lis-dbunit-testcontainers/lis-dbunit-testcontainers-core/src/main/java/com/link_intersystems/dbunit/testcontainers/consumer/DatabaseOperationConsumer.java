package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;
import org.dbunit.operation.DatabaseOperation;

import javax.sql.DataSource;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseOperationConsumer extends JdbcContainerAwareDataSetConsumer implements ChainableDataSetConsumer {

    private DatabaseOperation databaseOperation = DatabaseOperation.INSERT;
    private DefaultTable rowCache;

    private int rowCacheLimit = Integer.MAX_VALUE;

    public void setDatabaseOperation(DatabaseOperation databaseOperation) {
        this.databaseOperation = requireNonNull(databaseOperation);
    }

    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
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

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        rowCache = new DefaultTable(metaData);
        super.startTable(metaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        rowCache.addRow(values);
        if (isRowCacheLimitExceeded()) {
            flushRowCache();
            rowCache = new DefaultTable(rowCache.getTableMetaData());
        }
        super.row(values);
    }

    protected boolean isRowCacheLimitExceeded() {
        return rowCache != null && rowCache.getRowCount() >= rowCacheLimit;
    }

    protected void flushRowCache() throws DataSetException {
        if (rowCache == null || rowCache.getRowCount() == 0) {
            return;
        }

        IDatabaseConnection databaseConnection = getJdbcContainer().getDatabaseConnection();
        DatabaseOperation databaseOperation = getDatabaseOperation();
        processTable(databaseOperation, databaseConnection, rowCache);
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
    public void endTable() throws DataSetException {
        flushRowCache();

        rowCache = null;

        super.endTable();
    }

}
