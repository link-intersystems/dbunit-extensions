package com.link_intersystems.dbunit.repository.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcTableMetaData {

    private String catalog;
    private String schema;
    private String tableName;
    private String tableType;
    private String remarks;
    private String typeCatalog;
    private String typeSchama;
    private String typeName;
    private String selfReferencingColumnName;
    private String refGeneration;

    /**
     * @param tablesResultSet a ResultSet as returned by {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
     */
    public JdbcTableMetaData(ResultSet tablesResultSet) throws SQLException {
        catalog = tablesResultSet.getString("TABLE_CAT");
        schema = tablesResultSet.getString("TABLE_SCHEM");
        tableName = tablesResultSet.getString("TABLE_NAME");
        tableType = tablesResultSet.getString("TABLE_TYPE");
        remarks = tablesResultSet.getString("REMARKS");
        typeCatalog = tablesResultSet.getString("TYPE_CAT");
        typeSchama = tablesResultSet.getString("TYPE_SCHEM");
        typeName = tablesResultSet.getString("TYPE_NAME");
        selfReferencingColumnName = tablesResultSet.getString("SELF_REFERENCING_COL_NAME");
        refGeneration = tablesResultSet.getString("REF_GENERATION");
    }

    public String getCatalogName() {
        return catalog;
    }

    public String getSchemaName() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public String getRemarks() {
        return remarks;
    }

    public String getTypeCatalog() {
        return typeCatalog;
    }

    public String getTypeSchama() {
        return typeSchama;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getSelfReferencingColumnName() {
        return selfReferencingColumnName;
    }

    public String getRefGeneration() {
        return refGeneration;
    }
}
