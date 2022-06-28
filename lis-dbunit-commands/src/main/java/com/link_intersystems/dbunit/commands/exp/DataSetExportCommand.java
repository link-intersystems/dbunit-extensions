package com.link_intersystems.dbunit.commands.exp;

import com.link_intersystems.dbunit.dataset.consumer.CompositeDataSetConsumer;
import com.link_intersystems.dbunit.dataset.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.dataset.consumer.WriterDataSetConsumer;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetExportCommand implements DataSetConsumerSupport {

    private IDataSetConsumer dataSetConsumer;

    public interface ResultDataSetDecorator {

        public IDataSet decorate(IDataSet dataSet) throws DataSetException;
    }

    private final IDataSet dataSet;

    private String[] tables;
    private CompositeDataSetConsumer compositeDataSetConsumer = new CompositeDataSetConsumer();
    private TableOrder tableOrder;
    private ResultDataSetDecorator resultDecorator;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetExportCommand(IDataSet dataSet) {
        this.dataSet = requireNonNull(dataSet);
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }

    public void exec() throws DataSetException {
        IDataSet effectiveDataSet = dataSet;
        effectiveDataSet = filterTables(effectiveDataSet);
        effectiveDataSet = filterTablesContent(effectiveDataSet);
        effectiveDataSet = decorateResult(effectiveDataSet);
        effectiveDataSet = orderByDependencies(effectiveDataSet);

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(effectiveDataSet);
        producerAdapter.setConsumer(getDataSetConsumer());
        producerAdapter.produce();
    }

    private IDataSet decorateResult(IDataSet dataSet) throws DataSetException {
        if (resultDecorator != null) {
            return resultDecorator.decorate(dataSet);
        }
        return dataSet;
    }

    protected IDataSetConsumer getDataSetConsumer() {
        if (dataSetConsumer == null) {
            return new WriterDataSetConsumer();
        }
        return dataSetConsumer;
    }

    private IDataSet filterTablesContent(IDataSet dataSet) {
        return dataSet;
    }

    private IDataSet orderByDependencies(IDataSet dataSet) throws DataSetException {
        if (tableOrder != null) {
            String[] orderedTablesNames = tableOrder.orderTables(dataSet.getTableNames());
            SequenceTableFilter filter = new SequenceTableFilter(orderedTablesNames);
            return new FilteredDataSet(filter, dataSet);
        }
        return dataSet;
    }

    private IDataSet filterTables(IDataSet dataSet) throws AmbiguousTableNameException {
        if (tables != null && tables.length > 0) {
            return new FilteredDataSet(tables, dataSet);
        }
        return dataSet;
    }


    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }

    public void setResultDecorator(ResultDataSetDecorator resultDecorator) {
        this.resultDecorator = resultDecorator;
    }
}
