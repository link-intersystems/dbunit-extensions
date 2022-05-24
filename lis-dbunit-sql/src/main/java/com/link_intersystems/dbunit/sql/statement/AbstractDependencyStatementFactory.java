package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractDependencyStatementFactory implements DependencyStatementFactory{

    @Override
    public SqlStatement create(DatabaseConfig config, ITable sourceTable, Dependency.Edge sourceEdge, Dependency.Edge targetEdge) throws DataSetException {
        List<Column> sourceColumns = sourceEdge.getColumns();

        Set<List<Object>> statementArgs = new LinkedHashSet<>();

        int rowCount = sourceTable.getRowCount();

        for (int i = 0; i < rowCount; i++) {
            List<Object> ids = new ArrayList<>();

            for (int columnIndex = 0; columnIndex < sourceColumns.size(); columnIndex++) {
                Column column = sourceColumns.get(columnIndex);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                ids.add(columnValue);
            }

            statementArgs.add(ids);
        }

        String sql = createSql(sourceEdge, targetEdge, new ArrayList<>(statementArgs));

        List<Object> expandedArgs = statementArgs.stream().flatMap(List::stream).collect(Collectors.toList());

        return new SqlStatement(sql, expandedArgs);
    }

    protected abstract String createSql(Dependency.Edge sourceEdge, Dependency.Edge targetEdge, List<List<Object>> joinIds);
}
