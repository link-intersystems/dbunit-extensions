package com.link_intersystems.dbunit.dataset.browser.model;

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
}