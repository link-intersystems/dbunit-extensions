package com.link_intersystems.dbunit.repository.jdbc;

public interface ColumnDescription {

    public String getCatalogName();

    public String getSchemaName();

    public String getTableName();

    public String getColumnName();
}