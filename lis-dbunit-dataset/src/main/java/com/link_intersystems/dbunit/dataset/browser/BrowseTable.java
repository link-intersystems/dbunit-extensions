package com.link_intersystems.dbunit.dataset.browser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTable {

    private String tableName;
    private List<BrowseTableReference> references = new ArrayList<>();
    private TableCriteria tableCriteria;

    public BrowseTable(String tableName) {
        this.tableName = tableName;
    }

    public OngoingTableBrowse browse(String joinTableName) {
        return new OngoingTableBrowse(this, joinTableName);
    }

    public BrowseTable browseNatural(String joinTableName) {
        return browse(joinTableName).natural();
    }

    void addBrowse(BrowseTableReference join) {
        references.add(join);
    }

    public ColumnCriteriaBuilder with(String columnName) {
        return new ColumnCriteriaBuilder(this, columnName);
    }

    void addCriterion(TableCriterion tableCriterion) {
        if (tableCriteria == null) {
            tableCriteria = new TableCriteria();
        }
        tableCriteria.addCriterion(tableCriterion);
    }

    public String getTableName() {
        return tableName;
    }

    public TableCriteria getCriteria() {
        return tableCriteria;
    }

    public List<BrowseTableReference> getReferences() {
        return references;
    }
}
