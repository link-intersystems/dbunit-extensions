package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.format.LiteralFormat;
import com.link_intersystems.sql.statement.InsertSql;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;

import java.text.MessageFormat;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class InsertSqlBuilder {

    private String delimiter = ";";
    private SqlDialect sqlDialect;
    private String schema;

    public InsertSqlBuilder(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String createInsertSql(ITableMetaData metaData, Object[] values) throws DataSetException {
        String tableName = metaData.getTableName();
        if (schema != null) {
            tableName = schema + "." + tableName;
        }
        InsertSql insertSql = sqlDialect.createInsertSql(tableName);

        Column[] columns = metaData.getColumns();

        for (int colIndex = 0; colIndex < columns.length; ++colIndex) {
            Column column = columns[colIndex];
            try {
                String columnName = column.getColumnName();
                DataType dataType = column.getDataType();
                int sqlType = dataType.getSqlType();
                LiteralFormat literalFormat = sqlDialect.getLiteralFormat(sqlType);
                String literalValue = literalFormat.format(values[colIndex]);
                insertSql.addColumn(columnName, literalValue);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                String columnName = column.getColumnName();
                String msg = MessageFormat.format("Unable to write column {0}.{1}", tableName, columnName);
                throw new RuntimeException(msg, e);
            }

        }
        String sql = insertSql.toSqlString();

        String effectiveDelimiter = delimiter == null ? "" : delimiter;
        return sql + effectiveDelimiter;
    }

}
