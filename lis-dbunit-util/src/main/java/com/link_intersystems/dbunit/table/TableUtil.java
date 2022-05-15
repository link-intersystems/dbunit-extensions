package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.*;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableUtil {

    private ITable table;

    public TableUtil(ITable table) {
        this.table = table;
    }

    public RowList getRows() throws DataSetException {
        int rowCount = table.getRowCount();
        return getRows(0, rowCount);
    }

    public RowList getRows(String[] columnNames, Object[] columnValues) throws DataSetException {
        if (columnNames.length != columnValues.length) {
            throw new IllegalArgumentException("Column names must and column values must be of same size");
        }

        RowList rows = new RowList(table.getTableMetaData().getColumns());

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
        List<Object> rowObj = new ArrayList<>();

        for (int colIndex = 0; colIndex < columns.length; colIndex++) {
            Column column = columns[colIndex];
            String columnName = column.getColumnName();
            Object columnValue = table.getValue(row, columnName);
            rowObj.add(columnValue);
        }

        return new Row(tableMetaData.getColumns(), rowObj);
    }

    public RowList getRows(int startIndexInclusive, int endIndexExclusive) throws DataSetException {
        RowList rows = new RowList(table.getTableMetaData().getColumns());

        int rowCount = table.getRowCount();
        int maxIndexExclusive = Math.min(rowCount, endIndexExclusive);

        for (int rowIndex = startIndexInclusive; rowIndex < maxIndexExclusive; rowIndex++) {
            Row row = getRow(rowIndex);
            rows.add(row);
        }

        return rows;
    }

    public ITable createTable(List<Row> rows) throws DataSetException {
        RowList rowList = new RowList(table.getTableMetaData().getColumns());
        rowList.addAll(rows);
        DefaultTable defaultTable = new DefaultTable(table.getTableMetaData());

        for (Row row : rowList) {
            defaultTable.addRow(row.toArray());
        }

        return defaultTable;
    }

    public ITable[] splitTable(int partitionSize) throws DataSetException {

        int rowCount = table.getRowCount();
        ITable[] spittedTables = new ITable[(int) Math.ceil(rowCount / (double) partitionSize)];
        TableUtil tableUtil = new TableUtil(table);


        for (int i = 0, tableIndex = 0; tableIndex < spittedTables.length; i += partitionSize, tableIndex++) {
            List<Row> rows = tableUtil.getRows(i, i + partitionSize);
            ITable tablePartition = createTable(rows);
            spittedTables[tableIndex] = tablePartition;
        }

        return spittedTables;
    }
}
