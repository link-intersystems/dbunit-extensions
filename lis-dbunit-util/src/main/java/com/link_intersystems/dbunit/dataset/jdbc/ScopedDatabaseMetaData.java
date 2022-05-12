package com.link_intersystems.dbunit.dataset.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ScopedDatabaseMetaData {

    private final DatabaseMetaData databaseMetaData;
    private final String catalog;
    private final String schema;

    public ScopedDatabaseMetaData(DatabaseMetaData databaseMetaData, String catalog, String schema) {
        this.databaseMetaData = databaseMetaData;
        this.catalog = catalog;
        this.schema = schema;
    }

    public ResultSet getTables(String tableNamePattern) throws SQLException {
        return getTables(tableNamePattern, "TABLE");
    }

    public ResultSet getTables(String tableNamePattern, String... tableTypes) throws SQLException {
        return databaseMetaData.getTables(catalog, schema, tableNamePattern, tableTypes);
    }

    public ResultSet getColumns(String tableNamePattern) throws SQLException {
        return getColumns(tableNamePattern, "%");
    }

    public ResultSet getColumns(String tableNamePattern, String columnNamePattern) throws SQLException {
        return databaseMetaData.getColumns(catalog, schema, tableNamePattern, columnNamePattern);
    }

    public ResultSet getExportedKeys(String tableNamePattern) throws SQLException {
        return databaseMetaData.getExportedKeys(catalog,schema, tableNamePattern);
    }

    public ResultSet getImportedKeys(String tableNamePattern) throws SQLException {
        return databaseMetaData.getImportedKeys(catalog,schema, tableNamePattern);
    }
}
