package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Dependency {

    public static class Edge {
        private ITableMetaData tableMetaData;
        private List<Column> columns = new ArrayList<>();

        public Edge(ITableMetaData tableMetaData, List<Column> columns) throws DataSetException {
            if (!Arrays.asList(tableMetaData.getColumns()).containsAll(columns)) {
                throw new IllegalArgumentException("Columns must all belong to the given tableMetaData");
            }
            this.tableMetaData = tableMetaData;
            this.columns.addAll(columns);
        }

        public ITableMetaData getTableMetaData() {
            return tableMetaData;
        }

        public List<Column> getColumns() {
            return Collections.unmodifiableList(columns);
        }

        public String getTableName() {
            return getTableMetaData().getTableName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Edge edge = (Edge) o;
            return Objects.equals(tableMetaData, edge.tableMetaData) &&
                    Objects.equals(columns, edge.columns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableMetaData, columns);
        }

        @Override
        public String toString() {
            String columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.joining(", "));
            return tableMetaData.getTableName() + "(" + columnNames + ")";
        }
    }

    private final String name;
    private final Edge targetEdge;
    private final Edge sourceEdge;

    public Dependency(String name, Edge sourceEdge, Edge targetEdge) {
        this.name = name;
        this.sourceEdge = sourceEdge;
        this.targetEdge = targetEdge;
    }

    public String getName() {
        return name;
    }

    public Edge getSourceEdge() {
        return sourceEdge;
    }

    public Edge getTargetEdge() {
        return targetEdge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dependency that = (Dependency) o;
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
