package com.link_intersystems.dbunit.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableReferenceEdge {

    private String tableName;
    private List<String> columns = new ArrayList<>();

    public TableReferenceEdge(String tableName, List<String> columns) {
        this.tableName = tableName;
        this.columns.addAll(columns);
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableReferenceEdge edge = (TableReferenceEdge) o;
        return Objects.equals(tableName, edge.tableName) &&
                Objects.equals(columns, edge.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columns);
    }

    @Override
    public String toString() {
        return tableName + "(" + getColumns() + ")";
    }
}
