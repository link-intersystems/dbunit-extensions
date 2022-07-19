package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FilteredTableConsumer extends DefaultConsumer {

    private IDataSetConsumer subsequentConsumer;
    private ITableFilterSimple tableFilterSimple;
    private boolean tableAccepted;

    public FilteredTableConsumer(IDataSetConsumer subsequentConsumer, ITableFilterSimple tableFilterSimple) {
        this.subsequentConsumer = requireNonNull(subsequentConsumer);
        this.tableFilterSimple = requireNonNull(tableFilterSimple);
    }

    @Override
    public void startDataSet() throws DataSetException {
        subsequentConsumer.startDataSet();
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        String tableName = metaData.getTableName();
        tableAccepted = tableFilterSimple.accept(tableName);
        if (tableAccepted) {
            subsequentConsumer.startTable(metaData);
        }
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        if (tableAccepted) {
            subsequentConsumer.row(values);
        }
    }

    @Override
    public void endTable() throws DataSetException {
        if (tableAccepted) {
            subsequentConsumer.endTable();
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        subsequentConsumer.endDataSet();
    }
}
