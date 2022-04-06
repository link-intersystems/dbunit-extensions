package com.link_intersystems.dbunit.dataset.dbunit.dataset;

import com.link_intersystems.UnitTest;
import com.link_intersystems.dbunit.dataset.ColumnList;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@UnitTest
class ColumnListTest {

    private ColumnList columnList;
    private Column col1;
    private Column col2;

    @BeforeEach
    public void setUp() {
        col1 = new Column("col1", DataType.BIGINT);
        col2 = new Column("col2", DataType.VARCHAR);

        columnList = new ColumnList(col1, col2);
    }

    @Test
    void getColumn() {
        assertEquals(col1, columnList.getColumn("col1"));
        assertEquals(col2, columnList.getColumn("col2"));
        assertNull(columnList.getColumn("col3"));
    }

    @Test
    void getColumnIgnoreCase() {
        assertEquals(col1, columnList.getColumn("CoL1", true));
        assertEquals(col2, columnList.getColumn("CoL2", true));
        assertNull(columnList.getColumn("CoL3", true));
    }

    @Test
    void get() {
        assertEquals(col1, columnList.get(0));
        assertEquals(col2, columnList.get(1));
        assertThrows(IndexOutOfBoundsException.class, () -> columnList.get(2));
    }

    @Test
    void size() {
        assertEquals(2, columnList.size());
    }
}