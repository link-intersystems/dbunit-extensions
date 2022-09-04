package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingColumnBuild extends ColumnBuilder {

    private final ColumnListBuilder columnListBuilder;

    OngoingColumnBuild(ColumnListBuilder columnListBuilder, String columnName, DataType dataType) {
        super(columnName, dataType);
        this.columnListBuilder = columnListBuilder;
    }

    OngoingColumnBuild(ColumnListBuilder columnListBuilder, Column column) {
        super(column);
        this.columnListBuilder = columnListBuilder;
    }

    public Column build() {
        Column column = super.build();
        columnListBuilder.add(column);
        return column;
    }
}
