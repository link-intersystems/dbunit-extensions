package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BroadcastingDataSetConsumer implements IDataSetConsumer {

    private static interface DataSetConsumerMethod {

        public void invoke(IDataSetConsumer dataSetConsumer) throws DataSetException;
    }

    private List<IDataSetConsumer> targetConsumers = new ArrayList<>();

    public BroadcastingDataSetConsumer(IDataSetConsumer... dataSetConsumers) {
        this(Arrays.asList(dataSetConsumers));
    }

    public BroadcastingDataSetConsumer(List<IDataSetConsumer> dataSetConsumers) {
        this.targetConsumers.addAll(dataSetConsumers);
    }

    private void invokeTargetConsumers(DataSetConsumerMethod dataSetConsumerMethod) throws MultipleDataSetException {
        List<DataSetException> dataSetExceptions = new ArrayList<>();

        for (IDataSetConsumer targetConsumer : targetConsumers) {
            try {
                dataSetConsumerMethod.invoke(targetConsumer);
            } catch (DataSetException e) {
                dataSetExceptions.add(e);
            }
        }

        if (!dataSetExceptions.isEmpty()) {
            throw new MultipleDataSetException(dataSetExceptions);
        }
    }

    @Override
    public void startDataSet() throws DataSetException {
        invokeTargetConsumers(IDataSetConsumer::startDataSet);
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        invokeTargetConsumers(dsc -> dsc.startTable(iTableMetaData));
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        invokeTargetConsumers(dsc -> dsc.row(objects));
    }

    @Override
    public void endTable() throws DataSetException {
        invokeTargetConsumers(IDataSetConsumer::endTable);
    }

    @Override
    public void endDataSet() throws DataSetException {
        invokeTargetConsumers(IDataSetConsumer::endDataSet);
    }
}
