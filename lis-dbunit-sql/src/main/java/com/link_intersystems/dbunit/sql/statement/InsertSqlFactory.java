package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.sql.statement.InsertSql;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.text.MessageFormat;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class InsertSqlFactory {

    private String schema;

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public InsertSql createInsertSql(ITableMetaData metaData, Object[] values) throws DataSetException {
        String tableName = metaData.getTableName();
        if (schema != null) {
            tableName = schema + "." + tableName;
        }

        InsertSql insertSql = new InsertSql(tableName);

        Column[] columns = metaData.getColumns();

        for (int colIndex = 0; colIndex < columns.length; ++colIndex) {
            Column column = columns[colIndex];
            try {
                String columnName = column.getColumnName();
                insertSql.addColumn(columnName, values[colIndex]);
            } catch (Exception e) {
                String columnName = column.getColumnName();
                String msg = MessageFormat.format("Unable to write column {0}.{1}", tableName, columnName);
                throw new RuntimeException(msg, e);
            }
        }


        return insertSql;
    }
}
