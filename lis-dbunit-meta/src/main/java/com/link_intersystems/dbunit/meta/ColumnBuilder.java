package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

public class ColumnBuilder {

    private String columnName;
    private DataType dataType;
    private String sqlTypeName;
    private Column.Nullable nullable;
    private String defaultValue;
    private String remarks;
    private Column.AutoIncrement autoIncrement;

    public ColumnBuilder() {
    }

    public ColumnBuilder(Column column) {
        setColumnName(column.getColumnName());
        setDataType(column.getDataType());
        setSqlTypeName(column.getSqlTypeName());
        setNullable(column.getNullable());
        setDefaultValue(column.getDefaultValue());
        setRemarks(column.getRemarks());
        setAutoIncrement(column.getAutoIncrement());
    }

    public ColumnBuilder setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public ColumnBuilder setDataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public ColumnBuilder setSqlTypeName(String sqlTypeName) {
        this.sqlTypeName = sqlTypeName;
        return this;
    }

    public ColumnBuilder setNullable(Column.Nullable nullable) {
        this.nullable = nullable;
        return this;
    }

    public ColumnBuilder setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public ColumnBuilder setRemarks(String remarks) {
        this.remarks = remarks;
        return this;
    }

    public ColumnBuilder setAutoIncrement(Column.AutoIncrement autoIncrement) {
        this.autoIncrement = autoIncrement;
        return this;
    }

    public Column build() {
        return new Column(columnName, dataType, sqlTypeName, nullable, defaultValue, remarks, autoIncrement);
    }
}