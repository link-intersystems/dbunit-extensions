package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;

import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class RowList extends AbstractList<Row> {

    private Column[] columns;
    private List<Row> rows = new ArrayList<>();

    public RowList(Column[] columns) {
        this.columns = columns;
    }

    @Override
    public void add(int index, Row element) {
        checkRow(element);
        rows.add(index, element);
    }

    @Override
    public Row set(int index, Row element) {
        checkRow(element);
        return rows.set(index, element);
    }

    private void checkRow(Row row) {
        if (!Arrays.equals(columns, row.getColumns())) {
            String msg = MessageFormat.format("row {0} doesn't have the same columns than this row list {1}", Arrays.asList(row.getColumns()), Arrays.asList(columns));
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public Row remove(int index) {
        return rows.remove(index);
    }

    @Override
    public Row get(int index) {
        return rows.get(index);
    }

    @Override
    public int size() {
        return rows.size();
    }
}
