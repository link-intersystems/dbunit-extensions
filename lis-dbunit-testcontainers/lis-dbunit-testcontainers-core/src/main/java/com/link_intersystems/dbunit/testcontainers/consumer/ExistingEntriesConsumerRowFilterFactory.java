package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
import com.link_intersystems.dbunit.table.ColumnBuilder;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.table.PrimaryKey;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.database.DatabaseTableMetaDataAccess;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExistingEntriesConsumerRowFilterFactory extends DefaultContainerAwareDataSetConsumer implements ChainableDataSetConsumer, IRowFilterFactory {

    private Map<String, Collection<PrimaryKey>> primaryKeysByTableName = new HashMap<>();
    private Map<String, ITableMetaData> metaDataByTableName = new HashMap<>();

    protected ITableMetaData metaData;
    private ITableMetaData pkMetaData;

    @Override
    protected void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        super.startDataSet(jdbcContainer);
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        this.metaData = metaData;
        ITableMetaData pkMetaData = getPkMetaData();
        metaDataByTableName.put(metaData.getTableName(), pkMetaData);

        getExistingPrimaryKeys(pkMetaData);

        super.startTable(metaData);
    }

    private void getExistingPrimaryKeys(ITableMetaData pkMetaData) throws DataSetException {
        DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
        config.setTableFilter(tableName -> pkMetaData.getTableName().equals(tableName));
        DatabaseDataSetProducer dataSetProducer = new DatabaseDataSetProducer(getJdbcContainer().getDatabaseConnection(), config);

        Collection<PrimaryKey> primaryKeys = getPrimaryKeys(pkMetaData);

        dataSetProducer.setConsumer(new DefaultConsumer() {
            @Override
            public void row(Object[] values) throws DataSetException {
                PrimaryKey primaryKey = new PrimaryKey(pkMetaData, columnName -> values[pkMetaData.getColumnIndex(columnName)]);
                if (primaryKey.isEmpty()) {
                    return;
                }
                primaryKeys.add(primaryKey);
            }
        });
        dataSetProducer.produce();
    }

    @NotNull
    private Collection<PrimaryKey> getPrimaryKeys(ITableMetaData pkMetaData) {
        Collection<PrimaryKey> primaryKeys = primaryKeysByTableName.get(pkMetaData.getTableName());
        if (primaryKeys == null) {
            primaryKeys = new HashSet<>();
            primaryKeysByTableName.put(pkMetaData.getTableName(), primaryKeys);
        }
        return primaryKeys;
    }

    @Override
    public void endTable() throws DataSetException {
        metaData = null;
        pkMetaData = null;

        super.endTable();
    }

    protected ITableMetaData getPkMetaData() throws DataSetException {
        if (pkMetaData == null) {
            String tableName = metaData.getTableName();
            IDatabaseConnection databaseConnection = getJdbcContainer().getDatabaseConnection();
            ITableMetaData databaseMetaData = new DatabaseTableMetaDataAccess(tableName, databaseConnection);
            Column[] databaseColumns = databaseMetaData.getColumns();

            List<Column> dataTypeEnhancedColumns = new ArrayList<>();

            Column[] columns = metaData.getColumns();
            for (Column column : columns) {
                int databaseColumnIndex = databaseMetaData.getColumnIndex(column.getColumnName());
                Column databaseColumn = databaseColumns[databaseColumnIndex];

                ColumnBuilder columnBuilder = new ColumnBuilder(column);
                dataTypeEnhancedColumns.add(columnBuilder.setDataType(databaseColumn.getDataType()).build());
            }

            pkMetaData = new DefaultTableMetaData(
                    databaseMetaData.getTableName(),
                    dataTypeEnhancedColumns.toArray(new Column[0]),
                    Arrays.stream(databaseMetaData.getPrimaryKeys()).map(Column::getColumnName).toArray(String[]::new)
            );
        }

        return pkMetaData;
    }

    @Override
    public IRowFilter createRowFilter(ITableMetaData tableMetaData) {
        String tableName = tableMetaData.getTableName();
        Collection<PrimaryKey> primaryKeyFilter = primaryKeysByTableName.get(tableName);
        ITableMetaData metaData = metaDataByTableName.get(tableName);
        return rowValueProvider -> {
            if (metaData == null) {
                return true;
            }

            try {
                PrimaryKey actualRowPk = new PrimaryKey(metaData, rowValueProvider);
                return !primaryKeyFilter.contains(actualRowPk);
            } catch (DataSetException e) {
                return true;
            }
        };
    }
}
