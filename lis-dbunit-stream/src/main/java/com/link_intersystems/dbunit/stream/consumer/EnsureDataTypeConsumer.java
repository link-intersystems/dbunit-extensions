package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.Row;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EnsureDataTypeConsumer extends DefaultChainableDataSetConsumer {

    private ITableMetaData tableMetaData;

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        this.tableMetaData = iTableMetaData;
        super.startTable(iTableMetaData);
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        Row row = new Row(tableMetaData, objects);
        super.row(row.toArray());
    }
}
