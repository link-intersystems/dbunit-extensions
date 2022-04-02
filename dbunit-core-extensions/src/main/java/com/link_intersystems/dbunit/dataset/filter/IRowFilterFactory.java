package com.link_intersystems.dbunit.dataset.filter;

import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.IRowFilter;

/**
 * @author - René Link <rene.link@link-intersystems.com>
 */
public interface IRowFilterFactory {

    public IRowFilter createRowFilter(ITable table);
}
