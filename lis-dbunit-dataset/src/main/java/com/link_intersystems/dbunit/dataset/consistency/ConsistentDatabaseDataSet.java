package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.table.DatabaseTableReferenceLoader;
import com.link_intersystems.dbunit.table.TableReferenceTraversal;
import com.link_intersystems.jdbc.ConnectionMetaData;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDatabaseDataSet extends ConsistentDataSet {

    private static TableReferenceTraversal tableReferenceTraversal(IDatabaseConnection databaseConnection) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            ConnectionMetaData connectionMetaData = new ConnectionMetaData(connection);
            DatabaseTableReferenceLoader tableReferenceLoader = new DatabaseTableReferenceLoader(databaseConnection);
            return new TableReferenceTraversal(connectionMetaData, tableReferenceLoader);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public ConsistentDatabaseDataSet(IDatabaseConnection databaseConnection, ITable... tables) throws DataSetException {
        super(tableReferenceTraversal(databaseConnection), tables);
    }



    public ConsistentDatabaseDataSet(IDatabaseConnection databaseConnection, IDataSet sourceDataSet) throws DataSetException {
        super(sourceDataSet, tableReferenceTraversal(databaseConnection));
    }
}
