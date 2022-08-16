package com.link_intersystems.dbunit.stream.command;

import com.link_intersystems.dbunit.dataset.DataSetDecorator;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetPrinterConsumer;
import com.link_intersystems.dbunit.stream.producer.DataSetBuilder;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetSource;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceProducer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCommand implements DataSetConsumerSupport, DataSetProducerSupport {

    public static final String DEFAULT_NULL_REPLACEMENT = "[null]";

    private DataSetSource dataSetSource;
    private IDataSetConsumer dataSetConsumer;

    private String[] tables = new String[0];
    private TableOrder tableOrder;
    private DataSetDecorator resultDecorator;
    private IRowFilterFactory rowFilterFactory;

    private Map<Object, Object> replacementObjects;


    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetCommand(IDataSet sourceDataSet) {
        setDataSetProducer(sourceDataSet);
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }

    @Override
    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        DataSetSource dataSetSource;
        if (dataSetProducer instanceof DataSetSource) {
            dataSetSource = (DataSetSource) dataSetProducer;
        } else {
            dataSetSource = new DataSetSourceProducer(dataSetProducer);
        }

        setDataSetSource(dataSetSource);
    }

    public void setDataSetSource(DataSetSource dataSetSource) {
        this.dataSetSource = requireNonNull(dataSetSource);
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
        IDataSet dataSet = dataSetSource.get();

        dataSetBuilder.setDataSetProducer(dataSet);
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
