package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;

import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Row extends AbstractList<Object> {

    private Column[] columns;
    private List<Object> values;

    Row(Column[] columns, List<Object> values) {
        if (columns.length != values.size()) {
            String msg = MessageFormat.format("Column length {0} does not match values length {1}", columns.length, values.size());
            throw new IllegalArgumentException(msg);
        }

        this.columns = columns;
        this.values = values;
    }

    public Map<String, Object> toMap() {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();

        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i].getColumnName(), get(i));
        }

        return map;
    }

    @Override
    public Object get(int index) {
        return values.get(index);
    }

    @Override
    public int size() {
        return values.size();
    }

    public Column[] getColumns() {
        return columns;
    }
}
