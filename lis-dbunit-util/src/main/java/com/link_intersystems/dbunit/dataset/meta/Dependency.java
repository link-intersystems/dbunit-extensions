package com.link_intersystems.dbunit.dataset.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
}
