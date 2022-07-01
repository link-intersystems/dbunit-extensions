package com.link_intersystems.dbunit.table;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ColumnValueTest {

    @Test
    void nullName(){
        assertThrows(NullPointerException.class, () -> new ColumnValue(null, ""));
    }

    @Test
    void blankName(){
        assertThrows(IllegalArgumentException.class, () -> new ColumnValue(" ", ""));
    }

    @Test
    void getters() {
        ColumnValue columnValue = new ColumnValue("first_name", "PENELOPE");

        assertEquals("first_name", columnValue.getColumnName());
        assertEquals("PENELOPE", columnValue.getValue());
    }
}