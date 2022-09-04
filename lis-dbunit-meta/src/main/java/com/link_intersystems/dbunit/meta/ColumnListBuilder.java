package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnListBuilder {

    private List<Column> columns;

    public ColumnListBuilder() {
    }

    public ColumnListBuilder(Column[] columns) {
        this(Arrays.asList(columns));
    }

    public ColumnListBuilder(List<Column> columnList) {
        for (Column column : columnList) {
            newColumn(column).build();
        }
    }

    /**
     * @return a {@link OngoingColumnBuild} is a {@link ColumnBuilder} that can be used to
     * set more {@link Column} properties.Invoke {@link OngoingColumnBuild#build()} to add
     * the column to this {@link ColumnListBuilder}.
     */
    public OngoingColumnBuild newColumn(String columnName, DataType dataType) {
        return new OngoingColumnBuild(this, columnName, dataType);
    }

    public OngoingColumnBuild newColumn(Column column) {
        return new OngoingColumnBuild(this, column);
    }

    public ColumnList build() {
        if (columns == null) {
            return new ColumnList();
        }

        ColumnList columnList = new ColumnList(columns);
        columns = null;
        return columnList;
    }

    void add(Column column) {
        if (columns == null) {
            columns = new ArrayList<>();
        }
        columns.add(column);
    }
}
