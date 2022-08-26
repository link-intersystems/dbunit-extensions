package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.IRowFilterFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class RowFilterConsumer extends DefaultChainableDataSetConsumer {

    private IRowFilterFactory rowFilterFactory;
    private IRowFilter rowFilter;
    private ITableMetaData metaData;

    public void setRowFilterFactory(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = rowFilterFactory;
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        this.metaData = iTableMetaData;

        if (rowFilterFactory != null) {
            rowFilter = rowFilterFactory.createRowFilter(iTableMetaData);
        }

        super.startTable(iTableMetaData);
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        if (rowFilter != null) {
            if (rowFilter.accept(columnName -> objects[metaData.getColumnIndex(columnName)])) {
                super.row(objects);
            }
        } else {
            super.row(objects);
        }
    }
}
