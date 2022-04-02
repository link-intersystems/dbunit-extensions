package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.filter.IRowFilterFactory;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.IRowFilter;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author - René Link <rene.link@link-intersystems.com>
 */
public class RowFilterTableIterator implements ITableIterator {

    private final ITableIterator iterator;
    private IRowFilterFactory rowFilterFactory;
    private Map<ITable, ITable> filteredTables = new HashMap<>();

    public RowFilterTableIterator(ITableIterator iterator, IRowFilterFactory rowFilterFactory) {

        this.iterator = requireNonNull(iterator);
        this.rowFilterFactory = requireNonNull(rowFilterFactory);
    }

    @Override
    public boolean next() throws DataSetException {

        return iterator.next();
    }

    @Override
    public ITableMetaData getTableMetaData() throws DataSetException {

        return iterator.getTableMetaData();
    }

    @Override
    public ITable getTable() throws DataSetException {

        ITable table = iterator.getTable();
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
