package com.link_intersystems.dbunit.dataset.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcForeignKeyEntry {

    private String pkTableCategory;
    private String pkSchema;
    private String pkTableName;
    private String pkColumnName;


    private String fkTableCategory;
    private String fkSchema;
    private String fkTableName;
    private String fkColumnName;

    private short updateRule;
    private short deleteRule;

    private String fkName;
    private String pkName;

    private final short deferability;

    /**
     * @param foreignKeyResultSet a ResultSet as returned by {@link java.sql.DatabaseMetaData#getExportedKeys(String, String, String)} or {@link java.sql.DatabaseMetaData#getImportedKeys(String, String, String)}.
     */
    public JdbcForeignKeyEntry(ResultSet foreignKeyResultSet) throws SQLException {
        pkTableCategory = foreignKeyResultSet.getString("PKTABLE_CAT");
        pkSchema = foreignKeyResultSet.getString("PKTABLE_SCHEM");
        pkTableName = foreignKeyResultSet.getString("PKTABLE_NAME");
        pkColumnName = foreignKeyResultSet.getString("PKCOLUMN_NAME");

        fkTableCategory = foreignKeyResultSet.getString("FKTABLE_CAT");
        fkSchema = foreignKeyResultSet.getString("FKTABLE_SCHEM");
        fkTableName = foreignKeyResultSet.getString("FKTABLE_NAME");
        fkColumnName = foreignKeyResultSet.getString("FKCOLUMN_NAME");

        updateRule = foreignKeyResultSet.getShort("UPDATE_RULE");
        deleteRule = foreignKeyResultSet.getShort("DELETE_RULE");

        fkName = foreignKeyResultSet.getString("FK_NAME");
        pkName = foreignKeyResultSet.getString("PK_NAME");
        deferability = foreignKeyResultSet.getShort("DEFERRABILITY");
    }

    public String getPkTableCategory() {
        return pkTableCategory;
    }

    public String getPkSchema() {
        return pkSchema;
    }

    public String getPkTableName() {
        return pkTableName;
    }

    public String getPkColumnName() {
        return pkColumnName;
    }

    public String getFkTableCategory() {
        return fkTableCategory;
    }

    public String getFkSchema() {
        return fkSchema;
    }

    public String getFkTableName() {
        return fkTableName;
    }

    public String getFkColumnName() {
        return fkColumnName;
    }

    public short getUpdateRule() {
        return updateRule;
    }

    public short getDeleteRule() {
        return deleteRule;
    }

    public String getFkName() {
        return fkName;
    }

    public String getPkName() {
        return pkName;
    }

    public short getDeferability() {
        return deferability;
    }

    boolean isSameForeignKey(JdbcForeignKeyEntry jdbcForeignKeyEntry) {
        return getFkName().equals(jdbcForeignKeyEntry.getFkName());
    }

    @Override
    public String toString() {
        return "JdbcForeignKeyEntry{" +
                "fkTableCategory='" + fkTableCategory + '\'' +
                ", fkTableSchema='" + fkSchema + '\'' +
                ", fkTableName='" + fkTableName + '\'' +
                ", fkColumnName='" + fkColumnName + '\'' +
                '}';
    }

    public ColumnDescription getPkColumnDescription() {
        return new ColumnDescription() {
            @Override
            public String getCatalogName() {
                return getPkColumnName();
            }

            @Override
            public String getSchemaName() {
                return getPkSchema();
            }

            @Override
            public String getTableName() {
                return getPkTableName();
            }

            @Override
            public String getColumnName() {
                return getPkColumnName();
            }
        };
    }

    public ColumnDescription getFkColumnDescription() {
        return new ColumnDescription() {
            @Override
            public String getCatalogName() {
                return getFkColumnName();
            }

            @Override
            public String getSchemaName() {
                return getFkSchema();
            }

            @Override
            public String getTableName() {
                return getFkTableName();
            }

            @Override
            public String getColumnName() {
                return getFkColumnName();
            }
        };
    }
}
