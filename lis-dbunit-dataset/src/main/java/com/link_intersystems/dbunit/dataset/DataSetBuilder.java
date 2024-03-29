package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.meta.TableMetaDataBuilder;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.SequenceTableFilter;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetBuilder {

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
                DefaultDataSet defaultDataSet = new DefaultDataSet();

                ITableIterator iterator = dataSet.iterator();
                while (iterator.next()) {
                    ITable table = iterator.getTable();
                    ITableMetaData metaDataCopy = new TableMetaDataBuilder(table.getTableMetaData()).build();
                    DefaultTable defaultTable = new DefaultTable(metaDataCopy);
                    defaultTable.addTableRows(table);
                    defaultDataSet.addTable(table);
                }

                return defaultDataSet;
            }
        };

        protected abstract IDataSet apply(IDataSet dataSet) throws DataSetException;
    }

    private String[] tables = new String[0];
    private IRowFilterFactory rowFilterFactory;
    private DataSetDecorator resultDecorator;
    private Map<Object, Object> replacementObjects;
    private TableOrder tableOrder;

    private DataSetSupplier dataSetSource;

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

    public void setSourceDataSet(IDataSet dataSet) {
        setSourceDataSetSupplier(() -> dataSet);
    }

    public void setSourceDataSetSupplier(DataSetSupplier dataSetSource) {
        this.dataSetSource = requireNonNull(dataSetSource);
    }

    public IDataSet build() throws DataSetException {
        return build(BuildStrategy.COPY);
    }

    /**
     * @param buildStrategy the strategy to build the final {@link IDataSet}. Use {@link BuildStrategy#DECORATE} if you
     *                      want to get a decorated result that uses the decorator pattern to apply all builder rules.
     *                      The decorated result might consume more memory, but can reflect changes to
     *                      the source data set immediately (depending on the kind of source data set you use).
     *                      Use {@link BuildStrategy#COPY} if you want a clean copy of the decorated result. The
     *                      copied {@link IDataSet} will not reflect changes of the source data set, but since there
     *                      are no decorators anymore it consumes less memory.
     * @return the {@link IDataSet} based on the rules you set this {@link DataSetBuilder}.
     * @throws DataSetException if any exception occurs.
     */
    public IDataSet build(BuildStrategy buildStrategy) throws DataSetException {
        if (dataSetSource == null) {
            String msg = "Can not build data set. No DataSetProducer or DataSetSource set.";
            throw new DataSetException(msg);
        }

        IDataSet baseDataSet = dataSetSource.get();

        IDataSet tableFilteredDataSet = filterTables(baseDataSet);
        IDataSet tableContentFilteredDataSet = filterTablesContent(tableFilteredDataSet);
        IDataSet decoratedDataSet = decorateResult(tableContentFilteredDataSet);
        IDataSet nullReplacedDataSet = applyNullReplacement(decoratedDataSet);
        IDataSet orderedDataSet = orderByDependencies(nullReplacedDataSet);

        IDataSet buildDataSet = orderedDataSet;

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
