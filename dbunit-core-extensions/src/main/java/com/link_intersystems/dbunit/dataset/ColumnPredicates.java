package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

import java.util.function.Predicate;

/**
 * Convenience class that provides reusable predicates that can be applied to {@link Column}s.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public final class ColumnPredicates {

    private ColumnPredicates() {
    }

    public static Predicate<Column> byName(String columnName) {
        return c -> c.getColumnName().equals(columnName);
    }

    public static Predicate<Column> byNameIgnoreCase(String columnName) {
        return c -> c.getColumnName().equalsIgnoreCase(columnName);
    }

    public static Predicate<Column> byDataType(DataType dataType) {
        return c -> c.getDataType().equals(dataType);
    }
}
