package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dsl.TableBrowseNode;
import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import com.link_intersystems.dbunit.dsl.TableCriteria;
import com.link_intersystems.dbunit.dsl.TableCriterion;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SelectTableRefBuilder {

    private StringBuilder sb = new StringBuilder();
    private List<Object> arguments = new ArrayList<>();

    public SelectTableRefBuilder(TableBrowseRef tableBrowseRef) {
        sb.append("select * from " + tableBrowseRef.getTableName());

        TableCriteria criteria = tableBrowseRef.getCriteria();
        if (criteria != null) {
            sb.append(" where ");

            List<TableCriterion> criterions = criteria.getCriterions();
            Iterator<TableCriterion> iterator = criterions.iterator();

            while (iterator.hasNext()) {
                TableCriterion tableCriterion = iterator.next();
                String columnName = tableCriterion.getColumnName();
                String op = tableCriterion.getOp();
                Object value = tableCriterion.getValue();

                sb.append(columnName);

                switch (op) {
                    case "eq":
                        sb.append(" = ");
                }

                sb.append(" ? ");

                arguments.add(value);

                if(iterator.hasNext()){
                    sb.append(" and ");
                }
            }
        }
    }


    public void visit(TableBrowseRef sourceTableRef, TableBrowseNode browseNode) {
        sb.append(" left join ");
        TableBrowseRef targetTableRef = browseNode.getTargetTableRef();
        sb.append(targetTableRef.getTableName());

        sb.append(" on ");

        String[] sourceColumns = getSourceColumns(sourceTableRef, browseNode.getTargetTableRef(), browseNode);
        String[] targetColumns = getTargetColumns(sourceTableRef, browseNode.getTargetTableRef(), browseNode);

        for (int i = 0; i < sourceColumns.length; i++) {
            sb.append(sourceTableRef.getTableName());
            sb.append('.');
            sb.append(sourceColumns[i]);

            sb.append(" = ");

            sb.append(targetTableRef.getTableName());
            sb.append('.');
            sb.append(targetColumns[i]);
        }

    }

    private String[] getTargetColumns(TableBrowseRef sourceTableRef, TableBrowseRef targetTable, TableBrowseNode browseNode) {
        switch (sourceTableRef.getTableName()) {
            case "actor":
                switch (targetTable.getTableName()) {
                    case "film_actor":
                        return new String[]{"actor_id"};

                }
        }
        throw new IllegalStateException("target columns not found");
    }

    private String[] getSourceColumns(TableBrowseRef sourceTableRef, TableBrowseRef targetTable, TableBrowseNode browseNode) {
        switch (sourceTableRef.getTableName()) {
            case "actor":
                switch (targetTable.getTableName()) {
                    case "film_actor":
                        return new String[]{"actor_id"};

                }
        }

        throw new IllegalStateException("source columns not found");
    }

    public SqlStatement toSqlStatement() {
        return new SqlStatement(sb, arguments);
    }
}