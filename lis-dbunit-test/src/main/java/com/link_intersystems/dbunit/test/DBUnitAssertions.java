package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DBUnitAssertions {

    public static void assertDataSetEquals(IDataSet expected, IDataSet actual) throws DataSetException {
        assertArrayEquals(expected.getTableNames(), actual.getTableNames());

        String[] tableNames = expected.getTableNames();

        for (String tableName : tableNames) {
            ITable expectedTable = expected.getTable(tableName);
            ITable actualTable = actual.getTable(tableName);

            assertTablesEquals(expectedTable, actualTable);
        }

    }

    public static void assertTablesEquals(ITable expectedTable, ITable actualTable) throws DataSetException {
        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        ITableMetaData actualMetaData = actualTable.getTableMetaData();

        assertMetaDataEquals(expectedMetaData, actualMetaData);
        assertTableContentEquals(expectedTable, actualTable);
    }

    public static void assertTableContentEquals(ITable expectedTable, ITable actualTable) throws DataSetException {
        assertEquals(expectedTable.getRowCount(), actualTable.getRowCount());

        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        Column[] columns = expectedMetaData.getColumns();
        int rowCount = expectedTable.getRowCount();

        for (int row = 0; row < rowCount; row++) {
            for (Column column : columns) {
                String columnName = column.getColumnName();
                assertEquals(expectedTable.getValue(row, columnName), actualTable.getValue(row, columnName), "Value at row" + row + " and column '" + columnName + "'");
            }
        }
    }

    public static void assertMetaDataEquals(ITableMetaData expectedMetaData, ITableMetaData actualMetaData) throws DataSetException {
        assertEquals(expectedMetaData.getTableName(), actualMetaData.getTableName());
        assertArrayEquals(expectedMetaData.getColumns(), actualMetaData.getColumns());
        assertArrayEquals(expectedMetaData.getPrimaryKeys(), actualMetaData.getPrimaryKeys());
    }
}
