package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableMetaDataUtil;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FilterTableContentConsumer extends DefaultConsumer {

    private final IDataSetConsumer subsequentConsumer;
    private final IRowFilterFactory rowFilterFactory;
    private IRowFilter rowFilter;
    private ITableMetaData metaData;

    public FilterTableContentConsumer(IDataSetConsumer subsequentConsumer, IRowFilterFactory rowFilterFactory) {
        this.subsequentConsumer = requireNonNull(subsequentConsumer);
        this.rowFilterFactory = requireNonNull(rowFilterFactory);
    }

    @Override
    public void startDataSet() throws DataSetException {
        subsequentConsumer.startDataSet();
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        rowFilter = rowFilterFactory.createRowFilter(metaData);
        if (rowFilter == null) {
            rowFilter = r -> true;
        }
        this.metaData = metaData;
        subsequentConsumer.startTable(metaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        if (rowFilter.accept(getRowValueProvider(values))) {
            subsequentConsumer.row(values);
        }
    }

    @Override
    public void endTable() throws DataSetException {
        subsequentConsumer.endTable();
    }

    @Override
    public void endDataSet() throws DataSetException {
        subsequentConsumer.endDataSet();
    }

    private IRowValueProvider getRowValueProvider(Object[] values) {
        return s -> {
            TableMetaDataUtil tableMetaDataUtil = new TableMetaDataUtil(metaData);
            int columnIndex = tableMetaDataUtil.indexOf(s);
            return values[columnIndex];
        };
    }
}
