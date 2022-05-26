package com.link_intersystems.dbunit.dataset.browser;

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

                if (iterator.hasNext()) {
                    sb.append(" and ");
                }
            }
        }
    }

    public SqlStatement toSqlStatement() {
        return new SqlStatement(sb, arguments);
    }
}