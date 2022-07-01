package com.link_intersystems.dbunit.dataset.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractDataSetConsumerDelegate extends DefaultConsumer {

    @Override
    public void startDataSet() throws DataSetException {
        getDelegate().startDataSet();
    }

    @Override
    public void endDataSet() throws DataSetException {
        getDelegate().endDataSet();
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        getDelegate().startTable(iTableMetaData);
    }

    @Override
    public void endTable() throws DataSetException {
        getDelegate().endTable();
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        getDelegate().row(objects);
    }

    protected abstract IDataSetConsumer getDelegate();
}
