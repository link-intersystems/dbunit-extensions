package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ColumnListBuilderTest {

    private ColumnListBuilder columnListBuilder;

    @BeforeEach
    void setUp() {
        columnListBuilder = new ColumnListBuilder();
    }

    @Test
    void emptyList() {
        ColumnList columnList = columnListBuilder.build();
        assertEquals(0, columnList.size());
    }

    @Test
    void addColumn() {
        OngoingColumnBuild ongoingColumnBuild = columnListBuilder.newColumn("first_name", DataType.VARCHAR);
        ongoingColumnBuild.setColumnName("first_name")
                .setDataType(DataType.VARCHAR)
                .setSqlTypeName("VARCHAR")
                .setDefaultValue("N/A")
                .setAutoIncrement(Column.AutoIncrement.NO)
                .setNullable(Column.NO_NULLS)
                .setRemarks("Remarks")
                .build();


        ColumnList columnList = columnListBuilder.build();
        assertEquals(1, columnList.size());

        Column column = columnList.get(0);
        assertEquals("first_name", column.getColumnName());
        assertEquals(DataType.VARCHAR, column.getDataType());
        assertEquals("VARCHAR", column.getSqlTypeName());
        assertEquals("N/A", column.getDefaultValue());
        assertEquals("Remarks", column.getRemarks());
        assertEquals(Column.AutoIncrement.NO, column.getAutoIncrement());
        assertEquals(Column.NO_NULLS, column.getNullable());
    }

    @Test
    void addTemplateColumn() {
        Column templateColumn = new Column(
                "first_name",
                DataType.VARCHAR,
                "VARCHAR",
                Column.NO_NULLS,
                "N/A",
                "Remarks",
                Column.AutoIncrement.NO
        );
        OngoingColumnBuild elementBuilder = columnListBuilder.newColumn(templateColumn);
        elementBuilder.build();


        ColumnList columnList = columnListBuilder.build();
        assertEquals(1, columnList.size());

        Column column = columnList.get(0);
        assertEquals("first_name", column.getColumnName());
        assertEquals(DataType.VARCHAR, column.getDataType());
        assertEquals("VARCHAR", column.getSqlTypeName());
        assertEquals("N/A", column.getDefaultValue());
        assertEquals("Remarks", column.getRemarks());
        assertEquals(Column.AutoIncrement.NO, column.getAutoIncrement());
        assertEquals(Column.NO_NULLS, column.getNullable());
    }
}