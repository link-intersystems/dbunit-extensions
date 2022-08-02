package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;

import java.util.*;
import java.util.function.BiPredicate;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Row extends AbstractList<Object> {

    private List<Cell> cells;

    private BiPredicate<String, String> columnNameEquality = String::equals;

    public Row(List<Cell> cells) {
        this.cells = new ArrayList<>(requireNonNull(cells));
    }

    public Row ignoreCase() {
        Row ignoreCaseRow = new Row(cells);
        ignoreCaseRow.columnNameEquality = String::equalsIgnoreCase;
        return ignoreCaseRow;
    }

    public Map<String, Object> toMap() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        for (Cell cell : cells) {
            map.put(cell.getColumn().getColumnName(), cell.getValue());
        }

        return map;
    }

    @Override
    public Object get(int index) {
        return cells.get(index).getValue();
    }

    @Override
    public int size() {
        return cells.size();
    }

    public Column[] getColumns() {
        return cells.stream().map(Cell::getColumn).toArray(Column[]::new);
    }

    public Object getValue(String columnName) {
        Cell cell = getCell(columnName);
        return cell.getValue();
    }

    public Cell getCell(String columnName) {
        int indexOf = indexOfColumn(columnName);

        if (indexOf == -1) {
            throw new IllegalArgumentException("column named " + columnName + " does not exist");
        }

        return cells.get(indexOf);
    }

    public int indexOfColumn(String columnName) {

        for (int i = 0; i < cells.size(); i++) {
            Cell cell = getCell(i);
            if (columnNameEquality.test(cell.getColumnName(), columnName)) {
                return i;
            }
        }

        return -1;
    }

    public Cell getCell(int i) {
        return cells.get(i);
    }
}
