package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.sql.statement.JoinTableReferenceSqlFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.ITable;

import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultBrowseTableSqlFactory implements BrowseTableSqlFactory {

    private Map<String, SqlOperatorFormat> operatorToSqlMap = new HashMap<>();
    private TableReferenceResolver tableReferenceResolver;

    public DefaultBrowseTableSqlFactory(ConnectionMetaData connectionMetaData) {
        tableReferenceResolver = new DefaultTableReferenceResolverChain(connectionMetaData);
        registerOperators(operatorToSqlMap);
    }

    protected void registerOperators(Map<String, SqlOperatorFormat> operatorToSqlMap) {
        operatorToSqlMap.put("eq", new SingleValueOperatorFormat("="));
        operatorToSqlMap.put("gt", new SingleValueOperatorFormat(">"));
        operatorToSqlMap.put("gte", new SingleValueOperatorFormat(">="));
        operatorToSqlMap.put("lt", new SingleValueOperatorFormat("<"));
        operatorToSqlMap.put("lte", new SingleValueOperatorFormat("<="));
        operatorToSqlMap.put("in", new CollectionValuesOperatorFormat("in"));
        operatorToSqlMap.put("like", new SingleValueOperatorFormat("like"));
    }

    @Override
    public SqlStatement createSqlStatement(BrowseTable browseTable) {
        StringBuilder sb = new StringBuilder();
        List<Object> arguments = new ArrayList<>();
        sb.append("select * from ");
        sb.append(browseTable.getTableName());

        TableCriteria criteria = browseTable.getCriteria();
        if (criteria != null) {
            sb.append(" where ");

            appendCriteria(sb, arguments, criteria);
        }

        return new SqlStatement(sb.toString(), arguments);
    }

    private void appendCriteria(StringBuilder sb, List<Object> arguments, TableCriteria criteria) {
        List<TableCriterion> criterions = criteria.getCriterions();
        Iterator<TableCriterion> iterator = criterions.iterator();

        while (iterator.hasNext()) {
            TableCriterion tableCriterion = iterator.next();
            String columnName = tableCriterion.getColumnName();
            String op = tableCriterion.getOp();
            Object value = tableCriterion.getValue();

            sb.append(columnName);

            SqlOperatorFormat operatorFormat = operatorToSqlMap.get(op);
            SqlOperator sqlOperator = operatorFormat.format(value);
            sb.append(' ');
            sb.append(sqlOperator.getSql());
            arguments.addAll(sqlOperator.getArguments());

            if (iterator.hasNext()) {
                sb.append(" and ");
            }
        }
    }

    @Override
    public SqlStatement createSqlStatement(ITable sourceTable, BrowseTableReference targetTableReference) throws Exception {
        String sourceTableName = sourceTable.getTableMetaData().getTableName();
        TableReference tableReference = tableReferenceResolver.getTableReference(sourceTableName, targetTableReference);

        TableReference.Edge sourceEdge = tableReference.getSourceEdge();
        TableReference.Edge targetEdge = tableReference.getTargetEdge();

        JoinTableReferenceSqlFactory statementFactory = new JoinTableReferenceSqlFactory();

        SqlStatement sqlStatement = statementFactory.create(sourceTable, sourceEdge, targetEdge);

        StringBuilder sb = new StringBuilder(sqlStatement.getSql());
        List<Object> arguments = new ArrayList<>(sqlStatement.getArguments());

        BrowseTable targetTableRef = targetTableReference.getTargetBrowseTable();
        TableCriteria criteria = targetTableRef.getCriteria();
        if (criteria != null) {
            sb.append(" and ");
            appendCriteria(sb, arguments, criteria);
        }

        return new SqlStatement(sb.toString(), arguments);
    }


}
