package com.link_intersystems.dbunit.statement;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JoinDependencyStatementFactory implements DependencyStatementFactory {

    public static final DependencyStatementFactory INSTANCE = new JoinDependencyStatementFactory();

    @Override
    public SqlStatement create(DatabaseConfig config, ITable sourceTable, Dependency dependency) throws DataSetException {
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


        List<Object> statementArgs = new ArrayList<>();

        for (int i = 0; i < rowCount; i++) {
            for (int columnIndex = 0; columnIndex < sourceColumns.size(); columnIndex++) {
                Column column = sourceColumns.get(columnIndex);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                statementArgs.add(columnValue);
            }
        }

        return new SqlStatement(stmtBuilder, statementArgs);
    }

    private CharSequence getJoin(Dependency dependency) {
        StringBuilder joinBuilder = new StringBuilder();


        String sourceTableName = dependency.getSourceEdge().getTableName();
        String targetTableName = dependency.getTargetEdge().getTableName();

        List<ColumnJoin> joinColumns = ColumnJoin.of(dependency, sourceTableName, targetTableName);


        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(" JOIN ");
        joinBuilder.append(sourceTableName);
        joinBuilder.append(" ON ");
        joinBuilder.append(String.join(" AND ", columnJoins));

        return joinBuilder;
    }

}
