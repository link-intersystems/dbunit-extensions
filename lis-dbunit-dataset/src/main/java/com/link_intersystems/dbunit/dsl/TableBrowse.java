package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowse {

    private final TableBrowseRef tableBrowseNode;
    private final String joinTableName;

    public TableBrowse(TableBrowseRef tableBrowseNode, String joinTableName) {

        this.tableBrowseNode = tableBrowseNode;
        this.joinTableName = joinTableName;
    }

    public TableBrowseRef natural() {
        TableBrowseRef targetNode = new TableBrowseRef(joinTableName);
        TableBrowseNode tableBrowseNode = new TableBrowseNode(targetNode);
        this.tableBrowseNode.addBrowse(tableBrowseNode);
        return targetNode;
    }

    public TableBrowseBuilder on(String ...sourceColumnNames) {
        return new TableBrowseBuilder(tableBrowseNode, joinTableName, sourceColumnNames);
    }
}
