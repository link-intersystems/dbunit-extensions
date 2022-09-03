package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.ColumnBuilder;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CellTest {

    @Test
    void cast() throws TypeCastException {
        ColumnBuilder columnBuilder = new ColumnBuilder();
        columnBuilder.setDataType(DataType.VARCHAR);
        columnBuilder.setColumnName("amount");

        Cell cell = new Cell(columnBuilder.build(), "43.12");

        assertEquals("43.12", cell.getValue());

        Cell castedCell = cell.cast(DataType.DECIMAL);
        assertEquals(new BigDecimal("43.12"), castedCell.getValue());
    }

}