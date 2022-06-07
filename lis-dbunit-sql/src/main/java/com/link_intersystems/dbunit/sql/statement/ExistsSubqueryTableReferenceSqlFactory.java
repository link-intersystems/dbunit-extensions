package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.TableReference;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExistsSubqueryTableReferenceSqlFactory extends AbstractTableReferenceSqlFactory {

    public static final ExistsSubqueryTableReferenceSqlFactory INSTANCE = new ExistsSubqueryTableReferenceSqlFactory();

    @Override
    protected String createSql(TableReference.Edge sourceEdge, TableReference.Edge targetEdge, List<List<Object>> joinIds) {
        StringBuilder stmtBuilder = new StringBuilder("select ");

        stmtBuilder.append("*");

        stmtBuilder.append(" from ");
        String targetTableName = targetEdge.getTableName();
        stmtBuilder.append(targetTableName);
        stmtBuilder.append(" t");

        stmtBuilder.append(" where exists(");

        stmtBuilder.append("select * from ");
        String sourceTableName = sourceEdge.getTableName();
        stmtBuilder.append(sourceTableName);
        stmtBuilder.append(" s where ");

        CharSequence joinColumns = getJoin(sourceEdge, "s", targetEdge, "t");
        stmtBuilder.append(joinColumns);

        stmtBuilder.append(" and (");


        List<String> sourceColumns = sourceEdge.getColumns();
        List<String> columnCriteria = sourceColumns.stream()
                .map(c -> new ColumnCriteria("s", c).toString()).collect(Collectors.toList());
        String singleColumnCriteriaString = String.join(" and ", columnCriteria);


        String columnCriteriaString = String.join(" or ", Collections.nCopies(sourceColumns.size(), "(" + singleColumnCriteriaString + ")"));
        stmtBuilder.append(columnCriteriaString);

        stmtBuilder.append(")");

        stmtBuilder.append(")");

        return stmtBuilder.toString();
    }


    private CharSequence getJoin(TableReference.Edge sourceEdge, String sourceAlias, TableReference.Edge targetEdge, String targetAlias) {
        StringBuilder joinBuilder = new StringBuilder();

        List<ColumnJoin> joinColumns = ColumnJoin.of(sourceEdge, sourceAlias, targetEdge, targetAlias);

        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(String.join(" and ", columnJoins));

        return joinBuilder;
    }


}
