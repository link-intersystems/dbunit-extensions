package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowseBuilder {
    private TableBrowseRef sourceNode;
    private String targetTableName;
    private String[] sourceColumns;

    public TableBrowseBuilder(TableBrowseRef sourceNode, String targetTableName, String[] sourceColumns) {
        this.sourceNode = sourceNode;
        this.targetTableName = targetTableName;
        this.sourceColumns = sourceColumns;
    }

    public TableBrowseRef references(String... targetColumns) {
        TableBrowseRef targetNode = new TableBrowseRef(targetTableName);
        TableBrowseNode tableBrowseNode = new TableBrowseNode(targetNode);
        tableBrowseNode.setSourceColumns(sourceColumns);
        tableBrowseNode.setTargetColumns(targetColumns);
        sourceNode.addBrowse(tableBrowseNode);
        return targetNode;
    }
}
