package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.AbstractTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class SortedTable extends AbstractTable {

    private ITable sourceTable;
    private Comparator<Row> rowComparator;
    private int[] sortIndexMapping;

    public SortedTable(ITable sourceTable, Comparator<Row> rowComparator) {
        this.sourceTable = requireNonNull(sourceTable);
        this.rowComparator = requireNonNull(rowComparator);
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return sourceTable.getTableMetaData();
    }

    @Override
    public int getRowCount() {
        return sourceTable.getRowCount();
    }

    @Override
    public Object getValue(int row, String column) throws DataSetException {
        int sortedRowIndex = getSortedRowIndex(row);
        return sourceTable.getValue(sortedRowIndex, column);
    }

    private int getSortedRowIndex(int sourceIndex) throws DataSetException {
        if (sortIndexMapping == null) {
            TableUtil tableUtil = new TableUtil(sourceTable);
            RowList rowList = tableUtil.getRows();
            List<Row> sortedRows = new ArrayList<>(rowList);

            Collections.sort(sortedRows, rowComparator);

            sortIndexMapping = new int[sortedRows.size()];

            for (int i = 0; i < sortedRows.size(); i++) {
                Row sortedRow = sortedRows.get(i);
                int sortedIndex = rowList.indexOf(sortedRow);
                sortIndexMapping[i] = sortedIndex;
            }
        }

        return sortIndexMapping[sourceIndex];
    }
}