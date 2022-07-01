package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.dataset.consumer.WriterDataSetConsumer;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetMigrationCommand implements DataSetConsumerSupport {

    public static final String DEFAULT_NULL_REPLACEMENT = "[null]";

    private IDataSetConsumer dataSetConsumer;
    private String nullReplacement = DEFAULT_NULL_REPLACEMENT;
    private boolean nullHandlingEnabled = true;

    public void setNullReplacement(String nullReplacement) {
        this.nullReplacement = requireNonNull(nullReplacement);
    }

    public String getNullReplacement() {
        return nullReplacement;
    }

    public void setNullHandlingEnabled(boolean nullHandlingEnabled) {
        this.nullHandlingEnabled = nullHandlingEnabled;
    }

    public boolean isNullHandlingEnabled() {
        return nullHandlingEnabled;
    }

    public interface ResultDataSetDecorator {

        public IDataSet decorate(IDataSet dataSet) throws DataSetException;
    }

    private final IDataSet sourceDataSet;

    private String[] tables = new String[0];
    private TableOrder tableOrder;
    private ResultDataSetDecorator resultDecorator;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetMigrationCommand(IDataSet sourceDataSet) {
        this.sourceDataSet = requireNonNull(sourceDataSet);
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
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

    private IDataSet applyNullReplacement(IDataSet dataSet) {
        if (nullHandlingEnabled) {
            ReplacementDataSet replacementDataSet = new ReplacementDataSet(dataSet);
            replacementDataSet.addReplacementObject(null, nullReplacement);
            replacementDataSet.addReplacementObject(nullReplacement, ITable.NO_VALUE);
            replacementDataSet.setStrictReplacement(true);

            return replacementDataSet;
        }

        return dataSet;

    }

    private IDataSet decorateResult(IDataSet dataSet) throws DataSetException {
        if (resultDecorator == null) {
            return dataSet;
        }

        return resultDecorator.decorate(dataSet);
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
        if (tables.length == 0) {
            return dataSet;
        }

        return new FilteredDataSet(tables, dataSet);
    }


    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }

    public void setResultDecorator(ResultDataSetDecorator resultDecorator) {
        this.resultDecorator = resultDecorator;
    }
}
