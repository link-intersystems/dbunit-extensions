package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.text.MessageFormat;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * Convenience methods for table related queries.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableUtil implements Iterable<Row> {

    private ITable table;

    public TableUtil(ITable table) {
        this.table = requireNonNull(table);
    }

    public ITable getTable() {
        return table;
    }

    public RowList getRows() throws DataSetException {
        int rowCount = getTable().getRowCount();
        return getRows(0, rowCount);
    }

    public RowList getRows(ColumnValue[] columnValues) throws DataSetException {
        ITable table = getTable();
        RowList rows = new RowList(table.getTableMetaData());

        nextRow:
        for (int i = 0; i < table.getRowCount(); i++) {
            for (int j = 0; j < columnValues.length; j++) {
                ColumnValue columnValue = columnValues[j];
                Object cellValue = table.getValue(i, columnValue.getColumnName());
                if (!Objects.equals(columnValues[j].getValue(), cellValue)) {
                    continue nextRow;
                }
            }
            rows.add(getRow(i));
        }

        return rows;
    }

    public RowList getRows(String[] columnNames, Object... values) throws DataSetException {
        if (columnNames.length != values.length) {
            throw new IllegalArgumentException("Column names must and column values must be of same size");
        }

        ColumnValue[] columnValues = new ColumnValue[columnNames.length];

        for (int i = 0; i < columnNames.length; i++) {
            columnValues[i] = new ColumnValue(columnNames[i], values[i]);
        }

        return getRows(columnValues);
    }

    public Row getRowById(Object... idValues) throws DataSetException {
        ITable table = getTable();
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
                Object columnValue = this.table.getValue(i, primaryKeyColumn.getColumnName());
                if (!Objects.equals(idValues[j], columnValue)) {
                    continue nextRow;
                }
            }
            return getRow(i);
        }

        return null;
    }

    public Row getRow(int row) throws DataSetException {
        ITable table = getTable();
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
        ITable table = getTable();
        RowList rows = new RowList(table.getTableMetaData());

        int rowCount = table.getRowCount();
        int maxIndexExclusive = Math.min(rowCount, endIndexExclusive);

        for (int rowIndex = startIndexInclusive; rowIndex < maxIndexExclusive; rowIndex++) {
            Row row = getRow(rowIndex);
            rows.add(row);
        }

        return rows;
    }


    public ITable[] getPartitionedTables(int partitionSize) throws DataSetException {
        if (partitionSize < 1) {
            throw new IllegalArgumentException("partitionSize must be 1 or greater");
        }

        ITable table = getTable();
        int rowCount = table.getRowCount();
        ITable[] spittedTables = new ITable[(int) Math.ceil(rowCount / (double) partitionSize)];

        for (int i = 0, tableIndex = 0; tableIndex < spittedTables.length; i += partitionSize, tableIndex++) {
            RowList rows = getRows(i, i + partitionSize);
            ITable tablePartition = rows.toTable();
            spittedTables[tableIndex] = tablePartition;
        }

        return spittedTables;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {
            private int rowIndex = 0;

            @Override
            public boolean hasNext() {
                ITable table = getTable();
                return rowIndex < table.getRowCount();
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                try {
                    return getRow(rowIndex++);
                } catch (DataSetException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
