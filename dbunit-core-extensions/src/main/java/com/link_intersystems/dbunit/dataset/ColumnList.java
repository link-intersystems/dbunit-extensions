package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.Column;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

public class ColumnList extends AbstractList<Column> {

    private List<Column> columns;

    public ColumnList(Column... columns) {
        this(Arrays.asList(columns));
    }

    public ColumnList(List<Column> columns) {
        this.columns = columns;
    }

    public Column getColumn(String name) {
        return stream().filter(c -> c.getColumnName().equals(name)).findFirst().orElse(null);
    }

    @Override
    public Column get(int index) {
        return columns.get(index);
    }

    @Override
    public int size() {
        return columns.size();
    }
}
