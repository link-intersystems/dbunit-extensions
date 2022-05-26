package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingReferenceBrowse {
    private BrowseTable sourceNode;
    private String targetTableName;
    private String[] sourceColumns;

    public OngoingReferenceBrowse(BrowseTable sourceNode, String targetTableName, String[] sourceColumns) {
        this.sourceNode = sourceNode;
        this.targetTableName = targetTableName;
        this.sourceColumns = sourceColumns;
    }

    public BrowseTable references(String... targetColumns) {
        BrowseTable targetNode = new BrowseTable(targetTableName);
        BrowseTableReference tableBrowseNode = new BrowseTableReference(targetNode);
        tableBrowseNode.setSourceColumns(sourceColumns);
        tableBrowseNode.setTargetColumns(targetColumns);
        sourceNode.addBrowse(tableBrowseNode);
        return targetNode;
    }
}
