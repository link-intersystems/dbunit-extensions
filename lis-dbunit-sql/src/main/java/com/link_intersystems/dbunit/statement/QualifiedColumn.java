package com.link_intersystems.dbunit.statement;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class QualifiedColumn {

    private String tableName;
    private String columnName;

    public QualifiedColumn(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName);
        sb.append('.');
        sb.append(columnName);
        return sb.toString();
    }
}
