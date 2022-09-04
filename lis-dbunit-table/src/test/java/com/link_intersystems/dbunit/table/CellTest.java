package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CellTest {

    @Test
    void cast() throws TypeCastException {
        Column column = new Column("amount", DataType.VARCHAR);

        Cell cell = new Cell(column, "43.12");

        assertEquals("43.12", cell.getValue());

        Cell castedCell = cell.cast(DataType.DECIMAL);
        assertEquals(new BigDecimal("43.12"), castedCell.getValue());
    }

}