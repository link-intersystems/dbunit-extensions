package com.link_intersystems.dbunit.table;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnValue {

    private String columnName;
    private Object value;

    public ColumnValue(String columnName, Object value) {
        this.columnName = requireNonNull(columnName);
        if (columnName.trim().isEmpty()) {
            throw new IllegalArgumentException("columnName must not be blank");
        }

        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public Object getValue() {
        return value;
    }
}
