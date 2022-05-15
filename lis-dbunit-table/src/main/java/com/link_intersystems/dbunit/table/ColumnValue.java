package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnValue {

    private String columnName;
    private Object value;

    public ColumnValue(String columnName, Object value) {
        this.columnName = columnName;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public Object getValue() {
        return value;
    }
}
