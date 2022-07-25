package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableMetaDataUtil;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FilterTableContentConsumer extends DefaultDataSetConsumerPipe {

    private final IRowFilterFactory rowFilterFactory;
    private IRowFilter rowFilter;
    private ITableMetaData metaData;

    public FilterTableContentConsumer(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = requireNonNull(rowFilterFactory);
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        rowFilter = rowFilterFactory.createRowFilter(metaData);
        if (rowFilter == null) {
            rowFilter = r -> true;
        }
        this.metaData = metaData;
        super.startTable(metaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        if (rowFilter.accept(getRowValueProvider(values))) {
            super.row(values);
        }
    }

    private IRowValueProvider getRowValueProvider(Object[] values) {
        return s -> {
            TableMetaDataUtil tableMetaDataUtil = new TableMetaDataUtil(metaData);
            int columnIndex = tableMetaDataUtil.indexOf(s);
            return values[columnIndex];
        };
    }
}
