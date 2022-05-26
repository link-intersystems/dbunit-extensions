package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.TableReferenceEdge;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExistsSubqueryDependencyStatementFactory extends AbstractDependencyStatementFactory {

    public static final ExistsSubqueryDependencyStatementFactory INSTANCE = new ExistsSubqueryDependencyStatementFactory();

    @Override
    protected String createSql(TableReferenceEdge sourceEdge, TableReferenceEdge targetEdge, List<List<Object>> joinIds) {
        StringBuilder stmtBuilder = new StringBuilder("SELECT ");

        stmtBuilder.append("*");

        stmtBuilder.append(" FROM ");
        String targetTableName = targetEdge.getTableName();
        stmtBuilder.append(targetTableName);
        stmtBuilder.append(" t ");

        stmtBuilder.append(" WHERE EXISTS(");

        stmtBuilder.append("SELECT * FROM ");
        String sourceTableName = sourceEdge.getTableName();
        stmtBuilder.append(sourceTableName);
        stmtBuilder.append(" s WHERE ");

        CharSequence joinColumns = getJoin(sourceEdge, "s", targetEdge, "t");
        stmtBuilder.append(joinColumns);

        stmtBuilder.append(" AND (");


        List<String> sourceColumns = sourceEdge.getColumns();
        List<String> columnCriteria = sourceColumns.stream()
                .map(c -> new ColumnCriteria("s", c).toString()).collect(Collectors.toList());
        String singleColumnCriteriaString = String.join(" AND ", columnCriteria);


        String columnCriteriaString = String.join(" OR ", Collections.nCopies(sourceColumns.size() * joinIds.size(), "(" + singleColumnCriteriaString + ")"));
        stmtBuilder.append(columnCriteriaString);

        stmtBuilder.append(")");

        stmtBuilder.append(")");

        return stmtBuilder.toString();
    }


    private CharSequence getJoin(TableReferenceEdge sourceEdge, String sourceAlias, TableReferenceEdge targetEdge, String targetAlias) {
        StringBuilder joinBuilder = new StringBuilder();

        List<ColumnJoin> joinColumns = ColumnJoin.of(sourceEdge, sourceAlias, targetEdge, targetAlias);

        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(String.join(" AND ", columnJoins));

        return joinBuilder;
    }


}
