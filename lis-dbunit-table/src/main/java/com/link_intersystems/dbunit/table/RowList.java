package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.*;

import java.text.MessageFormat;
import java.util.*;

/**
 * A
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class RowList extends AbstractList<Row> {

    private final Column[] columns;
    private ITableMetaData tableMetaData;
    private List<Row> rows = new ArrayList<>();
    private Map<Row, Integer> indexMap;

    public RowList(ITableMetaData tableMetaData) throws DataSetException {
        this.tableMetaData = tableMetaData;
        columns = tableMetaData.getColumns();
    }

    public ITableMetaData getTableMetaData() {
        return tableMetaData;
    }

    @Override
    public int indexOf(Object o) {
        Map<Row, Integer> indexMap = getIndexMap();
        Integer index = indexMap.get(o);
        return index == null ? -1 : index;
    }

    private Map<Row, Integer> getIndexMap() {
        if (indexMap == null) {
            indexMap = new HashMap<>();
            int index = size() - 1;
            while (index > -1) {
                Row row = get(index);
                indexMap.put(row, index);
                index--;
            }
        }
        return indexMap;
    }

    @Override
    public void add(int index, Row element) {
        checkRow(element);
        rows.add(index, element);
        indexMap = null;
    }

    @Override
    public Row set(int index, Row element) {
        checkRow(element);
        Row previousElement = rows.set(index, element);
        indexMap = null;
        return previousElement;
    }

    private void checkRow(Row row) {
        if (!Arrays.equals(columns, row.getColumns())) {
            String msg = MessageFormat.format("row {0} doesn't have the same columns than this row list {1}", Arrays.asList(row.getColumns()), Arrays.asList(columns));
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public Row remove(int index) {
        Row previousElement = rows.remove(index);
        if (indexMap != null) {
            indexMap.values().remove(index);
        }
        return previousElement;
    }

    @Override
    public Row get(int index) {
        return rows.get(index);
    }

    @Override
    public int size() {
        return rows.size();
    }

    public ITable toTable() throws DataSetException {
        DefaultTable defaultTable = new DefaultTable(getTableMetaData());

        for (Row row : rows) {
            defaultTable.addRow(row.toArray());
        }

        return defaultTable;
    }
}
