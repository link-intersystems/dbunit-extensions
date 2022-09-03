package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableMetaDataReplacementConsumer extends DefaultChainableDataSetConsumer {

    private Function<String, ITableMetaData> tableMetaDataSupplier;

    public TableMetaDataReplacementConsumer(Function<String, ITableMetaData> tableMetaDataSupplier) {
        this.tableMetaDataSupplier = requireNonNull(tableMetaDataSupplier);
    }

    @Override
    public void startTable(ITableMetaData tableMetaData) throws DataSetException {
        String tableName = tableMetaData.getTableName();
        ITableMetaData effectiveTableMetaData = tableMetaDataSupplier.apply(tableName);
        if (effectiveTableMetaData == null) {
            effectiveTableMetaData = tableMetaData;
        }
        super.startTable(effectiveTableMetaData);
    }
}
