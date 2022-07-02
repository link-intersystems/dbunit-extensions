package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.DataSetDecorator;
import com.link_intersystems.dbunit.dataset.RowFilteredDataSet;
import com.link_intersystems.dbunit.dataset.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.dataset.consumer.DataSetPrinterConsumer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCommand implements DataSetConsumerSupport {

    public static final String DEFAULT_NULL_REPLACEMENT = "[null]";

    private IDataSetConsumer dataSetConsumer;

    private final IDataSet sourceDataSet;

    private String[] tables = new String[0];
    private TableOrder tableOrder;
    private DataSetDecorator resultDecorator;
    private IRowFilterFactory rowFilterFactory;

    private Map<Object, Object> replacementObjects;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetCommand(IDataSet sourceDataSet) {
        this.sourceDataSet = requireNonNull(sourceDataSet);
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }

    public void setTableContentFilter(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = rowFilterFactory;
    }

    public void setReplacementObjects(Map<Object, Object> replacementObjects) {
        this.replacementObjects = replacementObjects;
    }

    public void exec() throws DataSetException {
        IDataSet effectiveDataSet = sourceDataSet;

        effectiveDataSet = filterTables(effectiveDataSet);
        effectiveDataSet = filterTablesContent(effectiveDataSet);
        effectiveDataSet = decorateResult(effectiveDataSet);
        effectiveDataSet = applyNullReplacement(effectiveDataSet);
        effectiveDataSet = orderByDependencies(effectiveDataSet);

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(effectiveDataSet);
        producerAdapter.setConsumer(getDataSetConsumer());
        producerAdapter.produce();
    }

    protected IDataSet applyNullReplacement(IDataSet dataSet) {
        if (replacementObjects != null) {
            ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataSet);

            for (Map.Entry<Object, Object> replacement : replacementObjects.entrySet()) {
                replacementDataSet.addReplacementObject(replacement.getKey(), replacement.getValue());
            }
            replacementDataSet.setStrictReplacement(true);

            return replacementDataSet;
        }

        return dataSet;

    }

    protected IDataSet decorateResult(IDataSet dataSet) throws DataSetException {
        if (resultDecorator == null) {
            return dataSet;
        }

        return resultDecorator.decorate(dataSet);
    }

    protected IDataSetConsumer getDataSetConsumer() {
        if (dataSetConsumer == null) {
            return new DataSetPrinterConsumer();
        }

        return dataSetConsumer;
    }

    protected IDataSet filterTablesContent(IDataSet dataSet) {
        if (rowFilterFactory == null) {
            return dataSet;
        }

        return new RowFilteredDataSet(dataSet, rowFilterFactory);
    }

    protected IDataSet orderByDependencies(IDataSet dataSet) throws DataSetException {
        if (tableOrder != null) {
            String[] orderedTablesNames = tableOrder.orderTables(dataSet.getTableNames());
            SequenceTableFilter filter = new SequenceTableFilter(orderedTablesNames);
            return new FilteredDataSet(filter, dataSet);
        }
        return dataSet;
    }

    protected IDataSet filterTables(IDataSet dataSet) throws AmbiguousTableNameException {
        if (tables.length == 0) {
            return dataSet;
        }

        return new FilteredDataSet(tables, dataSet);
    }


    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }

    public void setResultDecorator(DataSetDecorator resultDecorator) {
        this.resultDecorator = resultDecorator;
    }
}
