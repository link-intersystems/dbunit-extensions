package com.link_intersystems.dbunit.dataset.browser.model;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTableCriteriaBuilder {
    private BrowseTable browseTable;
    private String columnName;

    BrowseTableCriteriaBuilder(BrowseTable browseTable, String columnName) {
        this.browseTable = browseTable;
        this.columnName = columnName;
    }

    public void eq(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "eq", value);
        browseTable.addCriterion(tableCriterion);
    }

    public void gt(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "gt", value);
        browseTable.addCriterion(tableCriterion);
    }

    public void gte(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "gte", value);
        browseTable.addCriterion(tableCriterion);
    }

    public void lt(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "lt", value);
        browseTable.addCriterion(tableCriterion);
    }

    public void lte(Object value) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "lte", value);
        browseTable.addCriterion(tableCriterion);
    }

    public void in(Object... values) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "in", values);
        browseTable.addCriterion(tableCriterion);
    }

    public void like(String likePattern) {
        TableCriterion tableCriterion = new TableCriterion(columnName, "like", likePattern);
        browseTable.addCriterion(tableCriterion);
    }
}
