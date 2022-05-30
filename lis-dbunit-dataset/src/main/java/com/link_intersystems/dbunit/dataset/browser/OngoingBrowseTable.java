package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingBrowseTable {

    private final BrowseTable tableBrowseNode;
    private final String targetTableName;

    public OngoingBrowseTable(BrowseTable tableBrowseNode, String targetTableName) {
        this.tableBrowseNode = tableBrowseNode;
        this.targetTableName = targetTableName;
    }

    public BrowseTable natural() {
        BrowseTable targetNode = new BrowseTable(targetTableName);
        BrowseTableReference tableBrowseNode = new BrowseTableReference(targetNode);
        this.tableBrowseNode.addBrowse(tableBrowseNode);
        return targetNode;
    }

    public OngoingReferenceBrowse on(String ...sourceColumnNames) {
        return new OngoingReferenceBrowse(tableBrowseNode, targetTableName, sourceColumnNames);
    }
}
