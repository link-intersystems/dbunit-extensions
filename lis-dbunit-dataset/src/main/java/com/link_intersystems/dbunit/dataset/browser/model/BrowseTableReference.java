package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.Arrays;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowseTableReference that = (BrowseTableReference) o;
        return Objects.equals(targetNode, that.targetNode) && Arrays.equals(sourceColumns, that.sourceColumns) && Arrays.equals(targetColumns, that.targetColumns);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(targetNode);
        result = 31 * result + Arrays.hashCode(sourceColumns);
        result = 31 * result + Arrays.hashCode(targetColumns);
        return result;
    }
}
