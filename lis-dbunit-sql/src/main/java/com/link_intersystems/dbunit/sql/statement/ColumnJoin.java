package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.dataset.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnJoin {

    public static List<ColumnJoin> of(Dependency.Edge sourceEdge, String sourceAlias, Dependency.Edge targetEdge, String targetAlias) {
        List<ColumnJoin> joinColumns = new ArrayList<>();

        List<Column> sourceColumns = sourceEdge.getColumns();
        ListIterator<Column> sourceColumnIterator = sourceColumns.listIterator();

        List<Column> targetColumns = targetEdge.getColumns();
        ListIterator<Column> targetColumnIterator = targetColumns.listIterator();

        while (sourceColumnIterator.hasNext() && targetColumnIterator.hasNext()) {
            Column sourceColumn = sourceColumnIterator.next();
            Column targetColumn = targetColumnIterator.next();

            QualifiedColumn qualifiedSourceColumn = toQualifiedColumn(sourceAlias, sourceColumn);
            QualifiedColumn qualifiedTargetColumn = toQualifiedColumn(targetAlias, targetColumn);

            joinColumns.add(new ColumnJoin(qualifiedSourceColumn, qualifiedTargetColumn));
        }

        return joinColumns;
    }

    private static QualifiedColumn toQualifiedColumn(String tableName, Column column) {
        String columnName = column.getColumnName();
        return new QualifiedColumn(tableName, columnName);
    }

    private final QualifiedColumn sourceColumn;
    private final QualifiedColumn targetColumn;

    public ColumnJoin(QualifiedColumn sourceColumn, QualifiedColumn targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(sourceColumn);
        sb.append(" = ");
        sb.append(targetColumn);

        return sb.toString();
    }
}
