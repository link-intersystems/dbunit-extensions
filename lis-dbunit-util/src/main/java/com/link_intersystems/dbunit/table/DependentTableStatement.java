package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DependentTableStatement {

    private static class QualifiedColumn {
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

    private static class JoinColumn {
        private ITableMetaData sourceTable;
        private Column sourceColumn;
        private ITableMetaData targetTable;
        private Column targetColumn;

        public JoinColumn(ITableMetaData sourceTable, Column sourceColumn, ITableMetaData targetTable, Column targetColumn) {
            this.sourceTable = sourceTable;
            this.sourceColumn = sourceColumn;
            this.targetTable = targetTable;
            this.targetColumn = targetColumn;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(new QualifiedColumn(sourceTable.getTableName(), sourceColumn.getColumnName()));
            sb.append(" = ");
            sb.append(new QualifiedColumn(targetTable.getTableName(), targetColumn.getColumnName()));

            return sb.toString();
        }
    }

    private Connection connection;

    public DependentTableStatement(Connection connection) {
        this.connection = connection;
    }

    public PreparedStatement create(ITable sourceTable, Dependency dependency) throws SQLException, DataSetException {
        Dependency.Edge sourceEdge = dependency.getSourceEdge();
        Dependency.Edge targetEdge = dependency.getTargetEdge();

        StringBuilder stmtBuilder = new StringBuilder("SELECT distinct ");

        ITableMetaData targetTableMetaData = targetEdge.getTableMetaData();
        stmtBuilder.append(targetTableMetaData.getTableName());
        stmtBuilder.append(".*");

        stmtBuilder.append(" FROM ");
        String targetTableName = targetTableMetaData.getTableName();
        stmtBuilder.append(targetTableName);


        CharSequence join = getJoin(dependency);

        stmtBuilder.append(join);
        stmtBuilder.append(" WHERE (");

        String sourceTableName = sourceEdge.getTableMetaData().getTableName();
        List<Column> sourceColumns = sourceEdge.getColumns();
        List<String> whereColumns = sourceColumns.stream()
                .map(sc -> new QualifiedColumn(sourceTableName, sc.getColumnName()))
                .map(QualifiedColumn::toString).collect(Collectors.toList());

        stmtBuilder.append(String.join(", ", whereColumns));

        stmtBuilder.append(") IN (");

        String whereSourceColumnsPart = String.join(", ", Collections.nCopies(sourceColumns.size(), "?"));
        int rowCount = sourceTable.getRowCount();
        String whereParameters = String.join(", ", Collections.nCopies(rowCount, "(" + whereSourceColumnsPart + ")"));
        stmtBuilder.append(whereParameters);

        stmtBuilder.append(")");

        PreparedStatement ps = connection.prepareStatement(stmtBuilder.toString());

        int paramIndex = 1;

        for (int i = 0; i < rowCount; i++) {
            for (int columnIndex = 0; columnIndex < sourceColumns.size(); columnIndex++) {
                Column column = sourceColumns.get(columnIndex);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                ps.setObject(paramIndex++, columnValue);
            }
        }

        return ps;
    }

    private CharSequence getJoin(Dependency dependency) {
        StringBuilder joinBuilder = new StringBuilder();


        List<JoinColumn> joinColumns = new ArrayList<>();

        Dependency.Edge sourceEdge = dependency.getSourceEdge();
        ITableMetaData sourceTable = sourceEdge.getTableMetaData();
        List<Column> sourceColumns = sourceEdge.getColumns();
        ListIterator<Column> sourceColumnIterator = sourceColumns.listIterator();

        Dependency.Edge targetEdge = dependency.getTargetEdge();
        ITableMetaData targetTable = targetEdge.getTableMetaData();
        List<Column> targetColumns = targetEdge.getColumns();
        ListIterator<Column> targetColumnIterator = targetColumns.listIterator();

        while (sourceColumnIterator.hasNext() && targetColumnIterator.hasNext()) {
            Column sourceColumn = sourceColumnIterator.next();
            Column targetColumn = targetColumnIterator.next();

            joinColumns.add(new JoinColumn(sourceTable, sourceColumn, targetTable, targetColumn));
        }

        List<String> columnJoins = joinColumns.stream().map(JoinColumn::toString).collect(Collectors.toList());

        joinBuilder.append(" JOIN ");
        joinBuilder.append(sourceTable.getTableName());
        joinBuilder.append(" ON ");
        joinBuilder.append(String.join(" AND ", columnJoins));

        return joinBuilder;
    }
}
