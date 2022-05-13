package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DependentEntityStatement {

    private Connection connection;

    public DependentEntityStatement(Connection connection) {
        this.connection = connection;
    }

    public PreparedStatement create(ITable sourceTable, Dependency dependency) throws SQLException, DataSetException {
        Dependency.Edge sourceEdge = dependency.getSourceEdge();
        Dependency.Edge targetEdge = dependency.getTargetEdge();

        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("SELECT distinct ");

        ITableMetaData targetTableMetaData = targetEdge.getTableMetaData();

        stmtBuilder.append(targetTableMetaData.getTableName());
        stmtBuilder.append(".*");

        stmtBuilder.append(" FROM ");
        String targetTableName = targetTableMetaData.getTableName();
        stmtBuilder.append(targetTableName);

        stmtBuilder.append(" JOIN ");

        String sourceTableName = sourceTable.getTableMetaData().getTableName();
        stmtBuilder.append(sourceTable.getTableMetaData().getTableName());

        stmtBuilder.append(" ON ");
        List<Column> sourceColumns = sourceEdge.getColumns();
        List<Column> targetColumns = targetEdge.getColumns();

        for (int i = 0; i < sourceColumns.size(); i++) {
            Column sourceColumn = sourceColumns.get(i);
            Column targetColumn = targetColumns.get(i);

            stmtBuilder.append(targetTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(targetColumn.getColumnName());

            stmtBuilder.append(" = ");

            stmtBuilder.append(sourceTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(sourceColumn.getColumnName());
            if (i < sourceColumns.size() - 1) {
                stmtBuilder.append(" AND ");
            }
        }

        stmtBuilder.append(" WHERE (");

        for (int i = 0; i < sourceColumns.size(); i++) {
            Column sourceColumn = sourceColumns.get(i);
            stmtBuilder.append(sourceTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(sourceColumn.getColumnName());
            if (i < sourceColumns.size() - 1) {
                stmtBuilder.append(", ");
            }
        }

        stmtBuilder.append(") IN (");

        String wherePart = String.join(", ", Collections.nCopies(sourceColumns.size(), "?"));

        int rowCount = sourceTable.getRowCount();
        for (int i = 0; i < rowCount; i++) {
            stmtBuilder.append(" ( ");
            stmtBuilder.append(wherePart);
            stmtBuilder.append(" ) ");
            if (i < rowCount - 1) {
                stmtBuilder.append(", ");
            }
        }

        stmtBuilder.append(")");

        PreparedStatement ps = connection.prepareStatement(stmtBuilder.toString());

        int paramIndex = 1;

        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < sourceColumns.size(); j++) {
                Column column = sourceColumns.get(j);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                ps.setObject(paramIndex++, columnValue);
            }
        }

        return ps;
    }
}
