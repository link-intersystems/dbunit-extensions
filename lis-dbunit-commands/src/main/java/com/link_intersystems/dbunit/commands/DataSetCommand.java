package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.DataSetBuilder;
import com.link_intersystems.dbunit.dataset.DataSetDecorator;
import com.link_intersystems.dbunit.dataset.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.dataset.consumer.DataSetPrinterConsumer;
import com.link_intersystems.dbunit.dataset.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCommand implements DataSetConsumerSupport, DataSetProducerSupport {

    public static final String DEFAULT_NULL_REPLACEMENT = "[null]";

    private IDataSetProducer dataSetProducer;
    private IDataSetConsumer dataSetConsumer;

    private String[] tables = new String[0];
    private TableOrder tableOrder;
    private DataSetDecorator resultDecorator;
    private IRowFilterFactory rowFilterFactory;

    private Map<Object, Object> replacementObjects;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetCommand(IDataSet sourceDataSet) throws DataSetException {
        setDataSetProducer(sourceDataSet);
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }

    @Override
    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        this.dataSetProducer = dataSetProducer;
    }

    public void setTableContentFilter(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = rowFilterFactory;
    }

    public void setReplacementObjects(Map<Object, Object> replacementObjects) {
        this.replacementObjects = replacementObjects;
    }

    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }

    public void setResultDecorator(DataSetDecorator resultDecorator) {
        this.resultDecorator = resultDecorator;
    }

    public void exec() throws DataSetException {
        DataSetBuilder dataSetBuilder = new DataSetBuilder();

        dataSetBuilder.setDataSetProducer(dataSetProducer);
        dataSetBuilder.setTables(tables);
        dataSetBuilder.setTableContentFilter(rowFilterFactory);
        dataSetBuilder.setResultDecorator(resultDecorator);
        dataSetBuilder.setReplacementObjects(replacementObjects);
        dataSetBuilder.setTableOrder(tableOrder);

        IDataSet sourceDataSet = dataSetBuilder.build(DataSetBuilder.BuildStrategy.DECORATE);

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(sourceDataSet);
        producerAdapter.setConsumer(getDataSetConsumer());
        producerAdapter.produce();
    }

    protected IDataSetConsumer getDataSetConsumer() {
        if (dataSetConsumer == null) {
            return new DataSetPrinterConsumer();
        }

        return dataSetConsumer;
    }
}
