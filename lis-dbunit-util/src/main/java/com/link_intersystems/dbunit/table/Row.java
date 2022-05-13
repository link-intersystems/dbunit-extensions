package com.link_intersystems.dbunit.table;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Row extends AbstractMap<String, Object> {

    private int row;
    private Map<String, Object> rowValues;

    Row(int row, Map<String, Object> rowValues) {
        this.row = row;
        this.rowValues = rowValues;
    }

    public int getRow() {
        return row;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return rowValues.entrySet();
    }
}
