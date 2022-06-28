package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DBUnitAssertions {

    public static final DBUnitAssertions STRICT =new DBUnitAssertions(true);
    public static final DBUnitAssertions LENIENT =new DBUnitAssertions(false);

    private boolean strictMode;

    public DBUnitAssertions(boolean strictMode) {
        this.strictMode = strictMode;
    }

    public boolean isStrictMode() {
        return strictMode;
    }

    public void assertDataSetEquals(IDataSet expected, IDataSet actual) throws DataSetException {
        assertEquals(new HashSet<>(Arrays.asList(expected.getTableNames())), new HashSet<>(Arrays.asList(actual.getTableNames())),
                "table names expected to be " + Arrays.asList(expected.getTableNames()) +
                        ", but were " + Arrays.asList(actual.getTableNames()));

        String[] tableNames = expected.getTableNames();

        for (String tableName : tableNames) {
            ITable expectedTable = expected.getTable(tableName);
            ITable actualTable = actual.getTable(tableName);

            assertTablesEquals(expectedTable, actualTable);
        }

    }

    public void assertTablesEquals(ITable expectedTable, ITable actualTable) throws DataSetException {
        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        ITableMetaData actualMetaData = actualTable.getTableMetaData();

        assertMetaDataEquals(expectedMetaData, actualMetaData);
        assertTableContentEquals(expectedTable, actualTable);
    }

    public void assertTableContentEquals(ITable expectedTable, ITable actualTable) throws DataSetException {
        assertEquals(expectedTable.getRowCount(), actualTable.getRowCount());

        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        Column[] columns = expectedMetaData.getColumns();
        Column[] actualColumns = actualTable.getTableMetaData().getColumns();
        int rowCount = expectedTable.getRowCount();

        for (int row = 0; row < rowCount; row++) {

            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                Column actualColumn = actualColumns[i];

                String columnName = column.getColumnName();

                Object expectedValue = expectedTable.getValue(row, columnName);
                Object actualValue = actualTable.getValue(row, columnName);

                if(!Objects.equals(column, actualColumn)){
                    if(!isStrictMode()){
                        DataType dataType = actualColumn.getDataType();
                        expectedValue = dataType.typeCast(expectedValue);
                        actualValue = dataType.typeCast(actualValue);
                    }
                }

                String tableName = actualTable.getTableMetaData().getTableName();
                assertEquals(expectedValue, actualValue, tableName +"[" + row + "][" + columnName + "]");
            }
        }
    }

    public void assertMetaDataEquals(ITableMetaData expectedMetaData, ITableMetaData actualMetaData) throws DataSetException {
        assertEquals(expectedMetaData.getTableName(), actualMetaData.getTableName());
        assertColumnsEquals(expectedMetaData.getColumns(), actualMetaData.getColumns());
        if(isStrictMode()){
            assertColumnsEquals(expectedMetaData.getPrimaryKeys(), actualMetaData.getPrimaryKeys());
        }
    }

    private void assertColumnsEquals(Column[] expected, Column[] actual) {
        assertEquals(expected.length, actual.length, "columns length");

        for (int i = 0; i < expected.length; i++) {
            Column expectedColumn = expected[i];
            Column actualColumn = actual[i];
            assertColumnEquals(expectedColumn, actualColumn);
        }
    }

    private void assertColumnEquals(Column expectedColumn, Column actualColumn) {
        if (isStrictMode()) {
            assertEquals(expectedColumn, actualColumn);
        } else {
            assertEquals(expectedColumn.getColumnName(), actualColumn.getColumnName());
        }
    }

    public void assertDataSetNotEquals(IDataSet unexpected, IDataSet actual) throws DataSetException {
        try {
            assertDataSetEquals(unexpected, actual);
            Assertions.fail("datasets are equals but should not");
        } catch (AssertionFailedError e) {
            // not equal
        }

    }
}
