package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.TableReferenceEdge;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JoinDependencyStatementFactory extends AbstractDependencyStatementFactory {

    public static final DependencyStatementFactory INSTANCE = new JoinDependencyStatementFactory();

    @Override
    protected String createSql(TableReferenceEdge sourceEdge, TableReferenceEdge targetEdge, List<List<Object>> joinIds) {
        StringBuilder stmtBuilder = new StringBuilder("SELECT distinct ");

        String targetTableName = targetEdge.getTableName();
        stmtBuilder.append(targetTableName);
        stmtBuilder.append(".*");

        stmtBuilder.append(" FROM ");
        stmtBuilder.append(targetTableName);


        CharSequence join = getJoin(sourceEdge, targetEdge);

        stmtBuilder.append(join);
        stmtBuilder.append(" WHERE (");

        String sourceTableName = sourceEdge.getTableName();
        List<String> sourceColumns = sourceEdge.getColumns();
        List<String> whereColumns = sourceColumns.stream()
                .map(sc -> new QualifiedColumn(sourceTableName, sc))
                .map(QualifiedColumn::toString).collect(Collectors.toList());

        stmtBuilder.append(String.join(", ", whereColumns));

        stmtBuilder.append(") IN (");


        String whereSourceColumnsPart = String.join(", ", Collections.nCopies(sourceColumns.size(), "?"));

        String whereParameters = String.join(", ", Collections.nCopies(joinIds.size() * sourceColumns.size(), "(" + whereSourceColumnsPart + ")"));
        stmtBuilder.append(whereParameters);

        stmtBuilder.append(")");

        return stmtBuilder.toString();
    }

    private CharSequence getJoin(TableReferenceEdge sourceEdge, TableReferenceEdge targetEdge) {
        StringBuilder joinBuilder = new StringBuilder();


        String sourceTableName = sourceEdge.getTableName();
        String targetTableName = targetEdge.getTableName();

        List<ColumnJoin> joinColumns = ColumnJoin.of(sourceEdge, sourceTableName, targetEdge, targetTableName);


        List<String> columnJoins = joinColumns.stream().map(ColumnJoin::toString).collect(Collectors.toList());

        joinBuilder.append(" JOIN ");
        joinBuilder.append(sourceTableName);
        joinBuilder.append(" ON ");
        joinBuilder.append(String.join(" AND ", columnJoins));

        return joinBuilder;
    }

}
