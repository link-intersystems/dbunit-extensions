package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.RowFilterTableIterator;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.IRowFilter;

/**
 * A {@link IDataSet} whose tables are filtered by {@link IRowFilter}s.
 * The row filters are created for each {@link ITable} using a {@link IRowFilterFactory}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class RowFilteredDataSet extends AbstractDataSet {

    private final IDataSet baseDataSet;
    private final IRowFilterFactory rowFilterFactory;

    public RowFilteredDataSet(IDataSet baseDataSet, IRowFilterFactory rowFilterFactory) {
        this.baseDataSet = baseDataSet;
        this.rowFilterFactory = rowFilterFactory;
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) throws DataSetException {
        ITableIterator baseTableIterator = reversed ? baseDataSet.reverseIterator() : baseDataSet.iterator();
        return new RowFilterTableIterator(baseTableIterator, rowFilterFactory);
    }
}
