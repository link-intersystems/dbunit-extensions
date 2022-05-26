package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnCriteriaBuilder {
    private BrowseTable tableRef;
    private String columnName;

    public ColumnCriteriaBuilder(BrowseTable tableRef, String columnName) {
        this.tableRef = tableRef;
        this.columnName = columnName;
    }

    public void eq(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "eq", value);
        tableRef.addCriterion(tableCriterion);
    }

    public void in(Object... values) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "in", values);
        tableRef.addCriterion(tableCriterion);
    }

    public void like(String likePattern) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "like", likePattern);
        tableRef.addCriterion(tableCriterion);
    }
}
