package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.text.MessageFormat;
import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableQueries {

    private ITable table;

    public TableQueries(ITable table) {
        this.table = table;
    }

    public List<Row> getRows(String[] columnNames, Object[] columnValues) throws DataSetException {
        if (columnNames.length != columnValues.length) {
            throw new IllegalArgumentException("Column names must and column values must be of same size");
        }

        List<Row> rows = new ArrayList<>();

        nextRow:
        for (int i = 0; i < table.getRowCount(); i++) {
            for (int j = 0; j < columnNames.length; j++) {
                String columnName = columnNames[j];
                Object columnValue = table.getValue(i, columnName);
                if (!Objects.equals(columnValues[j], columnValue)) {
                    continue nextRow;
                }
            }
            rows.add(getRow(i));
        }

        return rows;
    }

    public Row getRowById(Object... idValues) throws DataSetException {
        ITableMetaData tableMetaData = table.getTableMetaData();
        Column[] primaryKeys = tableMetaData.getPrimaryKeys();
        if (primaryKeys.length != idValues.length) {
            String msg = MessageFormat.format("Missing or to many idValues[{0}] for primary key columns {1}", idValues.length, Arrays.asList(primaryKeys));
            throw new IllegalArgumentException(msg);
        }

        nextRow:
        for (int i = 0; i < table.getRowCount(); i++) {
            for (int j = 0; j < primaryKeys.length; j++) {
                Column primaryKeyColumn = primaryKeys[j];
                Object columnValue = table.getValue(i, primaryKeyColumn.getColumnName());
                if (!Objects.equals(idValues[j], columnValue)) {
                    continue nextRow;
                }
            }
            return getRow(i);
        }

        return null;
    }

    public Row getRow(int row) throws DataSetException {
        ITableMetaData tableMetaData = table.getTableMetaData();
        Column[] columns = tableMetaData.getColumns();
        Map<String, Object> rowObj = new LinkedHashMap<>();

        for (int colIndex = 0; colIndex < columns.length; colIndex++) {
            Column column = columns[colIndex];
            String columnName = column.getColumnName();
            Object columnValue = table.getValue(row, columnName);
            rowObj.put(columnName, columnValue);
        }

        return new Row(row, rowObj);
    }
}
