package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.ITableFilterSimple;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FilterTableConsumer extends DefaultChainableDataSetConsumer {

    private ITableFilterSimple tableFilterSimple;
    private boolean tableAccepted;

    public FilterTableConsumer(ITableFilterSimple tableFilterSimple) {
        this.tableFilterSimple = requireNonNull(tableFilterSimple);
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        String tableName = metaData.getTableName();
        tableAccepted = tableFilterSimple.accept(tableName);
        if (tableAccepted) {
            super.startTable(metaData);
        }
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        if (tableAccepted) {
            super.row(values);
        }
    }

    @Override
    public void endTable() throws DataSetException {
        if (tableAccepted) {
            super.endTable();
        }
    }
}
