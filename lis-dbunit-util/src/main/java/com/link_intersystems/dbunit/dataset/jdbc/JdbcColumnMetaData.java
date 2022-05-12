package com.link_intersystems.dbunit.dataset.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcColumnMetaData {

    private String catalogName;
    private String schemaName;
    private String tableName;
    private String columnName;
    private int dataType;
    private String typeName;
    private int columnSize;
    private int decimalDigits;
    private int numPrecRadix;
    private int nullable;
    private String remarks;
    private String columnDefaultValue;
    private int charOctetLength;
    private final int ordinalPosition;
    private String isNullable;
    private String scopeCatalogName;
    private String scopeSchemaName;
    private String scopeTableName;
    private short sourceDataType;
    private String isAutoincrement;
    private String isGeneratedColumn;

    /**
     * @param tablesResultSet a ResultSet as returned by {@link java.sql.DatabaseMetaData#getTables(String, String, String, String[])}.
     */
    public JdbcColumnMetaData(ResultSet tablesResultSet) throws SQLException {
        catalogName = tablesResultSet.getString("TABLE_CAT");
        schemaName = tablesResultSet.getString("TABLE_SCHEM");
        tableName = tablesResultSet.getString("TABLE_NAME");
        columnName = tablesResultSet.getString("COLUMN_NAME");
        dataType = tablesResultSet.getInt("DATA_TYPE");
        typeName = tablesResultSet.getString("TYPE_NAME");
        columnSize = tablesResultSet.getInt("COLUMN_SIZE");
        decimalDigits = tablesResultSet.getInt("DECIMAL_DIGITS");
        numPrecRadix = tablesResultSet.getInt("NUM_PREC_RADIX");
        nullable = tablesResultSet.getInt("NULLABLE");
        remarks = tablesResultSet.getString("REMARKS");
        columnDefaultValue = tablesResultSet.getString("COLUMN_DEF");
        charOctetLength = tablesResultSet.getInt("CHAR_OCTET_LENGTH");
        ordinalPosition = tablesResultSet.getInt("ORDINAL_POSITION");
        isNullable = tablesResultSet.getString("IS_NULLABLE");
        scopeCatalogName = tablesResultSet.getString("SCOPE_CATALOG");
        scopeSchemaName = tablesResultSet.getString("SCOPE_SCHEMA");
        scopeTableName = tablesResultSet.getString("SCOPE_TABLE");
        sourceDataType = tablesResultSet.getShort("SOURCE_DATA_TYPE");
        isAutoincrement = tablesResultSet.getString("IS_AUTOINCREMENT");
        isGeneratedColumn = tablesResultSet.getString("IS_GENERATEDCOLUMN");
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getDataType() {
        return dataType;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public int getDecimalDigits() {
        return decimalDigits;
    }

    public int getNumPrecRadix() {
        return numPrecRadix;
    }

    public int getNullable() {
        return nullable;
    }

    public String getRemarks() {
        return remarks;
    }

    public String getColumnDefaultValue() {
        return columnDefaultValue;
    }

    public int getCharOctetLength() {
        return charOctetLength;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public String getIsNullable() {
        return isNullable;
    }

    public String getScopeCatalogName() {
        return scopeCatalogName;
    }

    public String getScopeSchemaName() {
        return scopeSchemaName;
    }

    public String getScopeTableName() {
        return scopeTableName;
    }

    public short getSourceDataType() {
        return sourceDataType;
    }

    public String getIsAutoincrement() {
        return isAutoincrement;
    }

    public String getIsGeneratedColumn() {
        return isGeneratedColumn;
    }
}
