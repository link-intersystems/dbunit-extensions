package com.link_intersystems.dbunit.dataset.consumer;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.operation.DatabaseOperation;

import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetConsumer extends CopyDataSetConsumer {

    private IDatabaseConnection databaseConnection;
    private DatabaseOperation databaseOperation;

    public DatabaseDataSetConsumer(IDatabaseConnection databaseConnection) {
        this(databaseConnection, DatabaseOperation.INSERT);
    }

    public DatabaseDataSetConsumer(IDatabaseConnection databaseConnection, DatabaseOperation databaseOperation) {
        this.databaseConnection = requireNonNull(databaseConnection);
        this.databaseOperation = requireNonNull(databaseOperation);
    }

    @Override
    protected void endDataSet(IDataSet dataSet) throws DataSetException {
        super.endDataSet(dataSet);

        try {
            databaseOperation.execute(databaseConnection, dataSet);
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }
    }
}
