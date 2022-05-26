package com.link_intersystems.dbunit.meta;

import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableReference {

    private final String name;
    private final TableReferenceEdge targetEdge;
    private final TableReferenceEdge sourceEdge;

    public TableReference(String name, TableReferenceEdge sourceEdge, TableReferenceEdge targetEdge) {
        this.name = name;
        this.sourceEdge = sourceEdge;
        this.targetEdge = targetEdge;
    }

    public String getName() {
        return name;
    }

    public TableReferenceEdge getSourceEdge() {
        return sourceEdge;
    }

    public TableReferenceEdge getTargetEdge() {
        return targetEdge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableReference that = (TableReference) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(targetEdge, that.targetEdge) &&
                Objects.equals(sourceEdge, that.sourceEdge);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, targetEdge, sourceEdge);
    }

    @Override
    public String toString() {
        return name + "<" + sourceEdge + " -> " + targetEdge + ">";
    }
}
