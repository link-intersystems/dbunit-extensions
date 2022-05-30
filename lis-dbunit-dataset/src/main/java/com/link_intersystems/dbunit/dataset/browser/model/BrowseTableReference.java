package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTableReference {

    private BrowseTable targetNode;
    private String[] sourceColumns = new String[0];
    private String[] targetColumns = new String[0];

    BrowseTableReference(BrowseTable targetNode) {
        this.targetNode = targetNode;
    }

    void setSourceColumns(String[] sourceColumns) {
        this.sourceColumns = Objects.requireNonNull(sourceColumns);
    }

    void setTargetColumns(String[] targetColumns) {
        this.targetColumns = Objects.requireNonNull(targetColumns);
    }

    public BrowseTable getTargetBrowseTable() {
        return targetNode;
    }

    public String[] getSourceColumns() {
        return sourceColumns;
    }

    public String[] getTargetColumns() {
        return targetColumns;
    }
}
