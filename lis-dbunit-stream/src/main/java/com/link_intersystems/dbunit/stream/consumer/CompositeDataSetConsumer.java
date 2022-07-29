package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CompositeDataSetConsumer implements IDataSetConsumer {

    private static interface DataSetConsumerMethod {

        public void invoke(IDataSetConsumer dataSetConsumer) throws DataSetException;
    }

    private LinkedHashSet<IDataSetConsumer> dataSetConsumers = new LinkedHashSet<>();

    public CompositeDataSetConsumer(IDataSetConsumer... dataSetConsumers) {
        this(Arrays.asList(dataSetConsumers));
    }

    public CompositeDataSetConsumer(List<IDataSetConsumer> dataSetConsumers) {
        this.dataSetConsumers.addAll(dataSetConsumers);
    }

    public void add(IDataSetConsumer dataSetConsumer) {
        dataSetConsumers.add(dataSetConsumer);
    }

    public void remove(IDataSetConsumer dataSetConsumer) {
        dataSetConsumers.remove(dataSetConsumer);
    }

    public boolean isEmpty() {
        return dataSetConsumers.isEmpty();
    }

    private void invokeConsumers(DataSetConsumerMethod dataSetConsumerMethod) throws CompositeDataSetException {
        List<DataSetException> dataSetExceptions = new ArrayList<>();

        for (IDataSetConsumer targetConsumer : dataSetConsumers) {
            try {
                dataSetConsumerMethod.invoke(targetConsumer);
            } catch (DataSetException e) {
                dataSetExceptions.add(e);
            }
        }

        if (!dataSetExceptions.isEmpty()) {
            throw new CompositeDataSetException(dataSetExceptions);
        }
    }

    @Override
    public void startDataSet() throws DataSetException {
        invokeConsumers(IDataSetConsumer::startDataSet);
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        invokeConsumers(dsc -> dsc.startTable(iTableMetaData));
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        invokeConsumers(dsc -> dsc.row(objects));
    }

    @Override
    public void endTable() throws DataSetException {
        invokeConsumers(IDataSetConsumer::endTable);
    }

    @Override
    public void endDataSet() throws DataSetException {
        invokeConsumers(IDataSetConsumer::endDataSet);
    }

}
