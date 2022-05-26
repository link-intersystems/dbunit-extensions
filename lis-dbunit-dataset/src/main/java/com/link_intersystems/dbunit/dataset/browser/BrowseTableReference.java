package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTableReference {

    private BrowseTable targetNode;
    private String[] sourceColumns;
    private String[] targetColumns;

    BrowseTableReference(BrowseTable targetNode) {
        this.targetNode = targetNode;
    }

    void setSourceColumns(String[] sourceColumns) {
        this.sourceColumns = sourceColumns;
    }

    void setTargetColumns(String[] targetColumns) {
        this.targetColumns = targetColumns;
    }

    public BrowseTable getTargetTableRef() {
        return targetNode;
    }

    public String[] getSourceColumns() {
        return sourceColumns;
    }

    public String[] getTargetColumns() {
        return targetColumns;
    }
}
