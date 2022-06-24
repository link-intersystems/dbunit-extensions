package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableCriterion {
    private String columnName;
    private String op;
    private Object value;

    TableCriterion(String columnName, String op, Object value) {
        this.columnName = columnName;
        this.op = op;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableCriterion criterion = (TableCriterion) o;
        return Objects.equals(columnName, criterion.columnName) && Objects.equals(op, criterion.op) && Objects.equals(value, criterion.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, op, value);
    }
}
