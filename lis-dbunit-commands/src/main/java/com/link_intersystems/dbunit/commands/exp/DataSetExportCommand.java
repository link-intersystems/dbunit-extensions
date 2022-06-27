package com.link_intersystems.dbunit.commands.exp;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDataSet;
import com.link_intersystems.dbunit.dataset.consumer.CompositeDataSetConsumer;
import com.link_intersystems.dbunit.dataset.consumer.WriterDataSetConsumer;
import com.link_intersystems.dbunit.table.TableReferenceLoader;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlWriter;

import java.io.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetExportCommand {

    private final IDataSet dataSet;

    private String[] tables;
    private CompositeDataSetConsumer compositeDataSetConsumer = new CompositeDataSetConsumer();
    private TableReferenceLoader tableReferenceLoader;
    private TableOrder tableOrder;

    public void setTables(String... tables) {
        this.tables = tables;
    }

    public DataSetExportCommand(IDataSet dataSet) {
        this.dataSet = requireNonNull(dataSet);
    }

    public DataSetExportCommand withFlatXmlConsumer(String file) throws IOException {
        return withFlatXmlConsumer(new File(file));
    }

    public DataSetExportCommand withFlatXmlConsumer(File file) throws IOException {
        return withFlatXmlConsumer(new FileOutputStream(file));
    }

    public DataSetExportCommand withFlatXmlConsumer(OutputStream outputStream) throws IOException {
        return withDataSetConsumer(new FlatXmlWriter(new BufferedOutputStream(outputStream)));
    }

    public DataSetExportCommand withDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.compositeDataSetConsumer.add(dataSetConsumer);
        return this;
    }

    public void exec() throws DataSetException {
        IDataSet effectiveDataSet = dataSet;
        effectiveDataSet = filterTables(effectiveDataSet);
        effectiveDataSet = filterTablesContent(effectiveDataSet);
        effectiveDataSet = consistentResult(effectiveDataSet);
        effectiveDataSet = orderByDependencies(effectiveDataSet);

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(effectiveDataSet);
        producerAdapter.setConsumer(getDataSetConsumer());
        producerAdapter.produce();
    }

    private IDataSet consistentResult(IDataSet dataSet) {
        if (tableReferenceLoader != null) {
            return new ConsistentDataSet(dataSet, tableReferenceLoader);
        }
        return dataSet;
    }


    protected IDataSetConsumer getDataSetConsumer() {
        if (!compositeDataSetConsumer.isEmpty()) {
            return compositeDataSetConsumer;
        }

        return new WriterDataSetConsumer();
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


    public void setConsistentResult(TableReferenceLoader tableReferenceLoader) {
        this.tableReferenceLoader = tableReferenceLoader;
    }


    public void setTableOrder(TableOrder tableOrder) {
        this.tableOrder = tableOrder;
    }
}
