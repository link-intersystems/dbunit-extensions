package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnCriteriaBuilder {
    private TableBrowseRef tableRef;
    private String columnName;

    public ColumnCriteriaBuilder(TableBrowseRef tableRef, String columnName) {
        this.tableRef = tableRef;
        this.columnName = columnName;
    }

    public void eq(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "eq", value);
        tableRef.addCriterion(tableCriterion);
    }
}
