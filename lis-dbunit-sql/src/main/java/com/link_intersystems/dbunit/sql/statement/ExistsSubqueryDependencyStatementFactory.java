package com.link_intersystems.dbunit.sql.statement;

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
public class ExistsSubqueryDependencyStatementFactory implements DependencyStatementFactory {

    public static final ExistsSubqueryDependencyStatementFactory INSTANCE = new ExistsSubqueryDependencyStatementFactory();

    public SqlStatement create(DatabaseConfig config, ITable sourceTable, Dependency dependency) throws DataSetException {
        Dependency.Edge sourceEdge = dependency.getSourceEdge();
        Dependency.Edge targetEdge = dependency.getTargetEdge();

        StringBuilder stmtBuilder = new StringBuilder("SELECT ");

        stmtBuilder.append("*");

        stmtBuilder.append(" FROM ");
        ITableMetaData targetTableMetaData = targetEdge.getTableMetaData();
        String targetTableName = targetTableMetaData.getTableName();
        stmtBuilder.append(targetTableName);
        stmtBuilder.append(" t ");

        stmtBuilder.append(" WHERE EXISTS(");

        stmtBuilder.append("SELECT * FROM ");
        ITableMetaData sourceTableMetadata = sourceEdge.getTableMetaData();
        String sourceTableName = sourceTableMetadata.getTableName();
        stmtBuilder.append(sourceTableName);
        stmtBuilder.append(" s WHERE ");

        CharSequence joinColumns = getJoin(dependency, "s", "t");
        stmtBuilder.append(joinColumns);

        stmtBuilder.append(" AND (");


        List<Column> sourceColumns = sourceEdge.getColumns();
        List<String> columnCriteria = sourceColumns.stream()
                .map(c -> new ColumnCriteria("s", c.getColumnName()).toString()).collect(Collectors.toList());
        String singleColumnCriteriaString = String.join(" AND ", columnCriteria);
        int rowCount = sourceTable.getRowCount();
        String columnCriteriaString = String.join(" OR ", Collections.nCopies(rowCount, "(" + singleColumnCriteriaString + ")"));
        stmtBuilder.append(columnCriteriaString);

        stmtBuilder.append(")");

        stmtBuilder.append(")");

        List<Object> args = new ArrayList<>();

        for (int i = 0; i < rowCount; i++) {
            for (int columnIndex = 0; columnIndex < sourceColumns.size(); columnIndex++) {
                Column column = sourceColumns.get(columnIndex);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                args.add(columnValue);
            }
        }

        return new SqlStatement(stmtBuilder, args);
    }


    private CharSequence getJoin(Dependency dependency, String sourceAlias, String targetAlias) {
        StringBuilder joinBuilder = new StringBuilder();

        List<ColumnJoin> joinColumns = ColumnJoin.of(dependency, sourceAlias, targetAlias);

        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(String.join(" AND ", columnJoins));

        return joinBuilder;
    }


}
