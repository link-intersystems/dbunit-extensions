package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.TableReferenceEdge;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnJoin {

    public static List<ColumnJoin> of(TableReferenceEdge sourceEdge, String sourceAlias, TableReferenceEdge targetEdge, String targetAlias) {
        List<ColumnJoin> joinColumns = new ArrayList<>();

        List<String> sourceColumns = sourceEdge.getColumns();
        ListIterator<String> sourceColumnIterator = sourceColumns.listIterator();

        List<String> targetColumns = targetEdge.getColumns();
        ListIterator<String> targetColumnIterator = targetColumns.listIterator();

        while (sourceColumnIterator.hasNext() && targetColumnIterator.hasNext()) {
            String sourceColumn = sourceColumnIterator.next();
            String targetColumn = targetColumnIterator.next();

            QualifiedColumn qualifiedSourceColumn = new QualifiedColumn(sourceAlias, sourceColumn);
            QualifiedColumn qualifiedTargetColumn = new QualifiedColumn(targetAlias, targetColumn);

            joinColumns.add(new ColumnJoin(qualifiedSourceColumn, qualifiedTargetColumn));
        }

        return joinColumns;
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
