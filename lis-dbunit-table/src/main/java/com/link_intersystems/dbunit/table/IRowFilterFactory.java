package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;

/**
 * A factory for creating {@link IRowFilter}s per table.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface IRowFilterFactory {

    public IRowFilter createRowFilter(ITableMetaData tableMetaData);
}
