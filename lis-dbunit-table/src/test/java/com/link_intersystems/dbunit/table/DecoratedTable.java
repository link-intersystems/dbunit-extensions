package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DecoratedTable extends AbstractTable {

    private final ITable sourceTable;
    private final ITableMetaData newTableMetaData;

    public DecoratedTable(ITable sourceTable, String... newPrimaryKeys) throws DataSetException {
        this(sourceTable, copyTableMetadata(sourceTable, newPrimaryKeys));

    }

    private static DefaultTableMetaData copyTableMetadata(ITable table, String[] primaryKeyNames) throws DataSetException {
        ITableMetaData tableMetaData = table.getTableMetaData();
        String tableName = tableMetaData.getTableName();
        Column[] columns = tableMetaData.getColumns();
        return new DefaultTableMetaData(tableName, columns, primaryKeyNames);
    }

    public DecoratedTable(ITable sourceTable, ITableMetaData newTableMetaData) {
        this.sourceTable = requireNonNull(sourceTable);
        this.newTableMetaData = requireNonNull(newTableMetaData);
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return newTableMetaData;
    }

    @Override
    public int getRowCount() {
        return sourceTable.getRowCount();
    }

    @Override
    public Object getValue(int rowIndex, String columnName) throws DataSetException {
        return sourceTable.getValue(rowIndex, columnName);
    }
}
