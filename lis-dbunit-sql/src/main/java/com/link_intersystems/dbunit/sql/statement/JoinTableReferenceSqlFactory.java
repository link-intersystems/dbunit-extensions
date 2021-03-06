package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.TableReference;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JoinTableReferenceSqlFactory extends AbstractTableReferenceSqlFactory {

    public static final TableReferenceSqlFactory INSTANCE = new JoinTableReferenceSqlFactory();

    @Override
    protected String createSql(TableReference.Edge sourceEdge, TableReference.Edge targetEdge, List<List<Object>> joinIds) {
        StringBuilder stmtBuilder = new StringBuilder("select distinct ");

        String targetTableName = targetEdge.getTableName();
        stmtBuilder.append(targetTableName);
        stmtBuilder.append(".*");

        stmtBuilder.append(" from ");
        stmtBuilder.append(targetTableName);


        CharSequence join = getJoin(sourceEdge, targetEdge);

        stmtBuilder.append(join);
        stmtBuilder.append(" where (");

        String sourceTableName = sourceEdge.getTableName();
        List<String> sourceColumns = sourceEdge.getColumns();
        List<String> whereColumns = sourceColumns.stream()
                .map(sc -> new QualifiedColumn(sourceTableName, sc))
                .map(QualifiedColumn::toString).collect(Collectors.toList());

        stmtBuilder.append(String.join(", ", whereColumns));

        stmtBuilder.append(") in (");


        String whereSourceColumnsPart = String.join(", ", Collections.nCopies(sourceColumns.size(), "?"));

        String whereParameters = String.join(", ", Collections.nCopies(joinIds.size(), "(" + whereSourceColumnsPart + ")"));
        stmtBuilder.append(whereParameters);

        stmtBuilder.append(")");

        return stmtBuilder.toString();
    }

    private CharSequence getJoin(TableReference.Edge sourceEdge, TableReference.Edge targetEdge) {
        StringBuilder joinBuilder = new StringBuilder();


        String sourceTableName = sourceEdge.getTableName();
        String targetTableName = targetEdge.getTableName();

        List<ColumnJoin> joinColumns = ColumnJoin.of(sourceEdge, sourceTableName, targetEdge, targetTableName);


        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(" join ");
        joinBuilder.append(sourceTableName);
        joinBuilder.append(" on ");
        joinBuilder.append(String.join(" and ", columnJoins));

        return joinBuilder;
    }

}
