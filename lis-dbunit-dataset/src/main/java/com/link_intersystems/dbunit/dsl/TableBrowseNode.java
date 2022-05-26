package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowseNode {

    private TableBrowseRef targetNode;
    private String[] sourceColumns;
    private String[] targetColumns;

    public TableBrowseNode(TableBrowseRef targetNode) {
        this.targetNode = targetNode;
    }

    void setSourceColumns(String[] sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    void setTargetColumns(String[] targetColumns) {
        this.targetColumns = targetColumns;
    }

    public TableBrowseRef getTargetTableRef() {
        return targetNode;
    }

    public String[] getSourceColumns() {
        return sourceColumns;
    }

    public String[] getTargetColumns() {
        return targetColumns;
    }
}
