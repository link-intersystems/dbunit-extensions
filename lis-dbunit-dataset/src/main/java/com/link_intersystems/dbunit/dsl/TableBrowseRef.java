package com.link_intersystems.dbunit.dsl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowseRef {

    private String tableName;
    private List<TableBrowseNode> browseNodes = new ArrayList<>();
    private TableCriteria tableCriteria;

    public TableBrowseRef(String tableName) {
        this.tableName = tableName;
    }

    public TableBrowse browse(String joinTableName) {
        return new TableBrowse(this, joinTableName);
    }

    void addBrowse(TableBrowseNode join) {
        browseNodes.add(join);
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

    public void accept(TableRefVisitor visitor) {
        visitor.visitRootTable(getTableName());

        browseNodes.forEach(bn -> visitor.visit(TableBrowseRef.this, bn));

    }

    public String getTableName() {
        return tableName;
    }

    public TableCriteria getCriteria() {
        return tableCriteria;
    }

    public List<TableBrowseNode> getBrowseNodes() {
        return browseNodes;
    }
}
