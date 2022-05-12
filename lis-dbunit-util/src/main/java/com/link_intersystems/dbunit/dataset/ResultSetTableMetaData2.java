package com.link_intersystems.dbunit.dataset;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.ResultSetTableMetaData;
import org.dbunit.dataset.DataSetException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ResultSetTableMetaData2 extends ResultSetTableMetaData {


    public ResultSetTableMetaData2(String tableName, ResultSet resultSet, IDatabaseConnection connection) throws DataSetException, SQLException {
        this(tableName, resultSet, connection, isCaseSensitiveConfigured(connection));
    }

    public ResultSetTableMetaData2(String tableName, ResultSet resultSet, IDatabaseConnection connection, boolean caseSensitiveMetaData) throws DataSetException, SQLException {
        super(tableName, resultSet, connection, caseSensitiveMetaData);
    }

    private static boolean isCaseSensitiveConfigured(IDatabaseConnection connection) {
        return connection.getConfig().getFeature("http://www.dbunit.org/features/caseSensitiveTableNames");
    }
}
