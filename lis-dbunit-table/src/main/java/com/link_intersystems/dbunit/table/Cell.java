package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Cell {

    private Column column;
    private Object value;

    public Cell(Column column, Object value) {
        this.column = requireNonNull(column);
        this.value = value;
    }

    public Object getValue() {
        return value;
    }

    public Cell cast(DataType dataType) throws TypeCastException {
        ColumnBuilder columnBuilder = new ColumnBuilder(column);
        columnBuilder.setDataType(dataType);
        Object castedValue = dataType.typeCast(getValue());
        return new Cell(columnBuilder.build(), castedValue);
    }

    public Column getColumn() {
        return column;
    }

    public String getColumnName() {
        return column.getColumnName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cell cell = (Cell) o;
        return column.equals(cell.column) && Objects.equals(value, cell.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, value);
    }
}
