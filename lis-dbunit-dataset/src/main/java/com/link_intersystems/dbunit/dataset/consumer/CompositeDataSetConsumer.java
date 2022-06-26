package com.link_intersystems.dbunit.dataset.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.LinkedHashSet;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CompositeDataSetConsumer implements IDataSetConsumer {

    private LinkedHashSet<IDataSetConsumer> dataSetConsumers = new LinkedHashSet<>();

    public void add(IDataSetConsumer dataSetConsumer) {
        dataSetConsumers.add(dataSetConsumer);
    }

    public void remove(IDataSetConsumer dataSetConsumer) {
        dataSetConsumers.remove(dataSetConsumer);
    }

    public boolean isEmpty() {
        return dataSetConsumers.isEmpty();
    }

    @Override
    public void startDataSet() throws DataSetException {
        for (IDataSetConsumer dataSetConsumer : dataSetConsumers) {
            dataSetConsumer.startDataSet();
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        for (IDataSetConsumer dataSetConsumer : dataSetConsumers) {
            dataSetConsumer.endDataSet();
        }
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        for (IDataSetConsumer dataSetConsumer : dataSetConsumers) {
            dataSetConsumer.startTable(iTableMetaData);
        }
    }

    @Override
    public void endTable() throws DataSetException {
        for (IDataSetConsumer dataSetConsumer : dataSetConsumers) {
            dataSetConsumer.endTable();
        }
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        for (IDataSetConsumer dataSetConsumer : dataSetConsumers) {
            dataSetConsumer.row(objects);
        }
    }

}
