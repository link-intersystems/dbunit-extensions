package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link ColumnList} provides filter and query methods for a list of {@link Column}s.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnList extends AbstractList<Column> {

    public static final Column[] EMPTY_COLUMN_ARRAY = new Column[0];
    private List<Column> columns;

    public ColumnList(Column... columns) {
        this(Arrays.asList(columns));
    }

    public ColumnList(List<Column> columns) {
        this.columns = columns;
    }

    /**
     * Get a {@link Column} by its name.
     *
     * @param name the column's name.
     */
    public Column getColumn(String name) {
        return getColumn(name, false);
    }

    /**
     * Get a {@link Column} by its name.
     *
     * @param name          the column's name.
     * @param caseSensitive ignore name's case.
     */
    public Column getColumn(String name, boolean caseSensitive) {
        Predicate<Column> columnPredicate = caseSensitive ? ColumnPredicates.byNameIgnoreCase(name) : ColumnPredicates.byName(name);
        return filter(columnPredicate).findFirst().orElse(null);
    }

    /**
     * Get a {@link Stream} of {@link Column}s filtered by the given {@link Predicate}.
     *
     * @see ColumnPredicates
     */
    public Stream<Column> filter(Predicate<Column> columnPredicate) {
        return stream().filter(columnPredicate);
    }

    @Override
    public Column get(int index) {
        return columns.get(index);
    }

    @Override
    public int size() {
        return columns.size();
    }

    public Column[] toArray() {
        return super.toArray(EMPTY_COLUMN_ARRAY);
    }

    public ColumnList copy() {
        ColumnListBuilder columnListBuilder = new ColumnListBuilder(this);
        return columnListBuilder.build();
    }

    public String[] toColumnNames() {
        return stream().map(Column::getColumnName).toArray(String[]::new);
    }
}
