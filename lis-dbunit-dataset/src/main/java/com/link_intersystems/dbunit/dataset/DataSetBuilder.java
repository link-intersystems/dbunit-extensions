package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.dataset.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetBuilder implements DataSetProducerSupport {

    public static enum BuildStrategy {
        DECORATE {
            @Override
            protected IDataSet apply(IDataSet dataSet) {
                return dataSet;
            }
        },
        COPY {
            @Override
            protected IDataSet apply(IDataSet dataSet) throws DataSetException {
                return copy(dataSet);
            }
        };

        protected abstract IDataSet apply(IDataSet dataSet) throws DataSetException;
    }

    public static IDataSet copy(IDataSet dataSet) throws DataSetException {
        DataSetProducerAdapter producer = new DataSetProducerAdapter(dataSet);
        return produce(producer);
    }

    public static IDataSet produce(IDataSetProducer dataSetProducer) throws DataSetException {
        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        dataSetProducer.setConsumer(copyDataSetConsumer);

        dataSetProducer.produce();

        return copyDataSetConsumer.getDataSet();
    }

    private String[] tables = new String[0];
    private IRowFilterFactory rowFilterFactory;
    private DataSetDecorator resultDecorator;
    private Map<Object, Object> replacementObjects;
    private TableOrder tableOrder;

    private IDataSetProducer dataSetProducer;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public void setTableContentFilter(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = rowFilterFactory;
    }

    public void setResultDecorator(DataSetDecorator resultDecorator) {
        this.resultDecorator = resultDecorator;
    }

    public void setReplacementObjects(Map<Object, Object> replacementObjects) {
        this.replacementObjects = replacementObjects;
    }

    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }

    @Override
    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        this.dataSetProducer = dataSetProducer;
    }

    public IDataSet build() throws DataSetException {
        return build(BuildStrategy.COPY);
    }

    public IDataSet build(BuildStrategy buildStrategy) throws DataSetException {
        if (dataSetProducer == null) {
            String msg = "Can not build data set. No dataSetProducer set.";
            throw new DataSetException(msg);
        }

        IDataSet baseDataSet = produce(dataSetProducer);

        IDataSet tableFilteredDataSet = filterTables(baseDataSet);
        IDataSet tableContentFilteredDataSet = filterTablesContent(tableFilteredDataSet);
        IDataSet decoratedDataSet = decorateResult(tableContentFilteredDataSet);
        IDataSet nullReplacedDataSet = applyNullReplacement(decoratedDataSet);
        IDataSet orderedDataSet = orderByDependencies(nullReplacedDataSet);

        IDataSet buildDataSet = orderedDataSet;

        // copy again to remove all decorators in order to reduce memory consumption.
        return buildStrategy.apply(buildDataSet);
    }


    protected IDataSet filterTables(IDataSet dataSet) throws AmbiguousTableNameException {
        if (tables.length == 0) {
            return dataSet;
        }

        return new FilteredDataSet(tables, dataSet);
    }

    protected IDataSet filterTablesContent(IDataSet dataSet) {
        if (rowFilterFactory == null) {
            return dataSet;
        }

        return new RowFilteredDataSet(dataSet, rowFilterFactory);
    }

    protected IDataSet decorateResult(IDataSet dataSet) throws DataSetException {
        if (resultDecorator == null) {
            return dataSet;
        }

        return resultDecorator.decorate(dataSet);
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

    protected IDataSet orderByDependencies(IDataSet dataSet) throws DataSetException {
        if (tableOrder != null) {
            String[] orderedTablesNames = tableOrder.orderTables(dataSet.getTableNames());
            SequenceTableFilter filter = new SequenceTableFilter(orderedTablesNames);
            return new FilteredDataSet(filter, dataSet);
        }
        return dataSet;
    }
}
