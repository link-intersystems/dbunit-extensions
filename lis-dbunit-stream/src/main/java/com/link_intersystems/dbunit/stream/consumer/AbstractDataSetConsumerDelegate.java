package com.link_intersystems.dbunit.stream.consumer;

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

    private static final DefaultConsumer NOOP_CONSUMER = new DefaultConsumer();

    @Override
    public void startDataSet() throws DataSetException {
        getNullSafeDelegate().startDataSet();
    }

    @Override
    public void endDataSet() throws DataSetException {
        getNullSafeDelegate().endDataSet();
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        getNullSafeDelegate().startTable(iTableMetaData);
    }

    @Override
    public void endTable() throws DataSetException {
        getNullSafeDelegate().endTable();
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        getNullSafeDelegate().row(objects);
    }

    private IDataSetConsumer getNullSafeDelegate(){
        IDataSetConsumer delegate = getDelegate();
        return delegate == null ? NOOP_CONSUMER : delegate;
    }

    protected abstract IDataSetConsumer getDelegate();
}
