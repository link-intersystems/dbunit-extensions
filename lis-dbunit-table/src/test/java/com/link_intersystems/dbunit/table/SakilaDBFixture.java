package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.test.H2Database;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.ITableFilterSimple;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SakilaDBFixture {

    private final IDataSet sakilaDataSet;

    public SakilaDBFixture(Connection connection) throws DatabaseUnitException {
        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(connection);
            ITableFilterSimple filterSimple = name -> H2Database.SYSTEM_TABLE_PREDICATE.negate().test(name);
            sakilaDataSet = new DatabaseDataSet(databaseConnection, false, filterSimple);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }

    }

    public ITable getTable(String name) throws DataSetException {
        IDataSet sakilaDataSet = getSakilaDataSet();
        return sakilaDataSet.getTable(name);
    }

    public IDataSet getSakilaDataSet() {
        return sakilaDataSet;
    }

    public ITable[] getSplittedTables(String tableName, int splitSize) throws DataSetException {
        ITable table = getTable(tableName);
        TableUtil tableUtil = new TableUtil(table);
        return tableUtil.getPartitionedTables(splitSize);

    }
}
