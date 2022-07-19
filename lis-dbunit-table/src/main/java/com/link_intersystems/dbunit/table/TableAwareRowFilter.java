package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.IRowFilter;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableAwareRowFilter extends IRowFilter {

    default boolean accept(ITable table, IRowValueProvider rowValueProvider) {
        return accept(rowValueProvider);
    }
}
