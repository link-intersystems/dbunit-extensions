package com.link_intersystems.dbunit.stream.consumer.db;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.operation.DatabaseOperation;

import java.sql.SQLException;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetConsumer extends CopyDataSetConsumer {

    private IDatabaseConnection databaseConnection;
    private DatabaseOperation databaseOperation;
    private DatabaseDataSet databaseDataSet;

    private boolean lenient;

    public DatabaseDataSetConsumer(IDatabaseConnection databaseConnection) {
        this(databaseConnection, DatabaseOperation.INSERT);
    }

    public DatabaseDataSetConsumer(IDatabaseConnection databaseConnection, DatabaseOperation databaseOperation) {
        this.databaseConnection = requireNonNull(databaseConnection);
        this.databaseOperation = requireNonNull(databaseOperation);
    }

    public DatabaseOperation getDatabaseOperation() {
        return databaseOperation;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public boolean isLenient() {
        return lenient;
    }

    @Override
    public void startDataSet() throws DataSetException {
        super.startDataSet();
        try {
            databaseDataSet = new DatabaseDataSet(databaseConnection, false);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    protected ITableMetaData copyMetaData(ITableMetaData metaData) throws DataSetException {
        String tableName = metaData.getTableName();

        ITableMetaData databaseMetaData = null;
        try {
            databaseMetaData = databaseDataSet.getTableMetaData(tableName);
        } catch (DataSetException e) {
            if (!isLenient()) {
                throw new DataSetException("Target database doesn't have a table named '" + tableName + "'", e);
            }
        }

        if (databaseMetaData != null) {
            if (databaseMetaData.getColumns().length != metaData.getColumns().length) {
                if (!isLenient()) {
                    throw new DataSetException("Target database table '" + tableName + "' columns differ:\ndb     : "
                            + Arrays.asList(databaseMetaData.getColumns()) + "\nsource : " + Arrays.asList(metaData.getColumns()));
                }
            }
            metaData = databaseMetaData;
        }
        return super.copyMetaData(metaData);
    }

    @Override
    protected void endDataSet(IDataSet dataSet) throws DataSetException {
        super.endDataSet(dataSet);

        try {
            getDatabaseOperation().execute(databaseConnection, dataSet);
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }
    }
}
