package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.IRowFilter;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * A {@link ITableIterator} that creates {@link IRowFilter}s for each {@link ITable} it iterates over.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class RowFilterTableIterator implements ITableIterator {

    private final ITableIterator baseIterator;

    private IRowFilterFactory rowFilterFactory;
    private Map<ITable, ITable> filteredTables = new HashMap<>();

    /**
     * @param baseIterator     the base iterator to decorate.
     * @param rowFilterFactory the factory that can create row filters for tables.
     */
    public RowFilterTableIterator(ITableIterator baseIterator, IRowFilterFactory rowFilterFactory) {
        this.baseIterator = requireNonNull(baseIterator);
        this.rowFilterFactory = requireNonNull(rowFilterFactory);
    }

    @Override
    public boolean next() throws DataSetException {
        return baseIterator.next();
    }

    @Override
    public ITableMetaData getTableMetaData() throws DataSetException {
        return baseIterator.getTableMetaData();
    }

    @Override
    public ITable getTable() throws DataSetException {
        ITable table = baseIterator.getTable();
        return computeIfAbsent(table);
    }

    /*
     * Sadly I can not use a method reference to {@link Map#computeIfAbsent(Object, Function)}.
     * Since the {@link RowFilterTable} declares to throw a {@link DataSetException}. Even though the
     * RowFilterTable doesn't seem to throw it.
     */
    private ITable computeIfAbsent(ITable table) throws DataSetException {
        ITable filteredTable = filteredTables.get(table);

        if (filteredTable == null) {
            filteredTable = createFilteredTable(table);
            filteredTables.put(table, filteredTable);
        }

        return filteredTable;
    }

    private ITable createFilteredTable(ITable baseTable) throws DataSetException {
        IRowFilter rowFilter = rowFilterFactory.createRowFilter(baseTable);
        return new RowFilterTable(baseTable, rowFilter);
    }
}
