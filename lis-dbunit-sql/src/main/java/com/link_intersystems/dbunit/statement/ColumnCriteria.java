package com.link_intersystems.dbunit.statement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnCriteria {

    private QualifiedColumn qualifiedColumn;

    public ColumnCriteria(String table, String columnName) {
        this(new QualifiedColumn(table, columnName));
    }

    public ColumnCriteria(QualifiedColumn qualifiedColumn) {
        this.qualifiedColumn = qualifiedColumn;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(qualifiedColumn);
        sb.append(" = ?");

        return sb.toString();
    }
}
