package com.link_intersystems.dbunit.meta;

import com.link_intersystems.jdbc.ColumnDescription;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.ForeignKey;
import com.link_intersystems.jdbc.ForeignKeyEntry;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableReferenceRepository {

    private ConnectionMetaData connectionMetaData;

    public TableReferenceRepository(ConnectionMetaData connectionMetaData) {
        this.connectionMetaData = Objects.requireNonNull(connectionMetaData);
    }

    public List<TableReference> getIncomingReferences(String tableName) throws SQLException {
        List<ForeignKey> exportedKeys = connectionMetaData.getExportedKeys(tableName);
        return toReferences(exportedKeys);
    }

    public List<TableReference> getOutgoingReferences(String tableName) throws SQLException {
        List<ForeignKey> importedKeys = connectionMetaData.getImportedKeys(tableName);
        return toReferences(importedKeys);
    }


    private List<TableReference> toReferences(List<ForeignKey> foreignKeys) {
        List<TableReference> references = new ArrayList<>();

        for (ForeignKey foreignKey : foreignKeys) {
            TableReference reference = toReferences(foreignKey);
            references.add(reference);
        }

        return references;
    }

    private TableReference toReferences(ForeignKey foreignKey) {
        TableReferenceEdge sourceEdge = getReferenceEdge(foreignKey, ForeignKeyEntry::getFkColumnDescription);
        TableReferenceEdge targetEdge = getReferenceEdge(foreignKey, ForeignKeyEntry::getPkColumnDescription);

        String name = foreignKey.getName();

        return new TableReference(name, sourceEdge, targetEdge);
    }

    private TableReferenceEdge getReferenceEdge(ForeignKey foreignKey, Function<ForeignKeyEntry, ColumnDescription> columnGetter) {
        List<String> columns = new ArrayList<>();
        String tableName = null;

        for (ForeignKeyEntry entry : foreignKey) {
            ColumnDescription columnDescription = columnGetter.apply(entry);
            columns.add(columnDescription.getColumnName());
            tableName = columnDescription.getTableName();
        }

        return new TableReferenceEdge(tableName, columns);
    }
}
