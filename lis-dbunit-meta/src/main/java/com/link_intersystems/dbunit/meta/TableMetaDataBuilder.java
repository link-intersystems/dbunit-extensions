package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;

import java.util.Arrays;
import java.util.Objects;

public class TableMetaDataBuilder {

    private String tableName;
    private Column[] columns;
    private String[] pkColumns;

    public TableMetaDataBuilder(String tableName) {
        setTableName(tableName);
    }

    public TableMetaDataBuilder(ITableMetaData tableMetaData) throws DataSetException {
        setTableName(tableMetaData.getTableName());

        ColumnListBuilder columnListBuilder = new ColumnListBuilder(tableMetaData.getColumns());
        ColumnList columnList = columnListBuilder.build();
        setColumns(columnList.toArray());

        Column[] primaryKeys = tableMetaData.getPrimaryKeys();
        String[] pkNames = Arrays.stream(primaryKeys).map(Column::getColumnName).toArray(String[]::new);
        setPkColumns(pkNames);
    }

    public TableMetaDataBuilder setTableName(String tableName) {
        this.tableName = Objects.requireNonNull(tableName);
        return this;
    }

    public TableMetaDataBuilder setColumns(Column... columns) {
        this.columns = columns.clone();
        return this;
    }

    public TableMetaDataBuilder setPkColumns(String... pkColumns) {
        this.pkColumns = pkColumns.clone();
        return this;
    }

    public ITableMetaData build() {
        return new DefaultTableMetaData(tableName, columns, pkColumns);
    }
}