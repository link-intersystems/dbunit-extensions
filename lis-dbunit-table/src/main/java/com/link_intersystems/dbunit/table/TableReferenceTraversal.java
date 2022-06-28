package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceList;
import com.link_intersystems.jdbc.TableReferenceMetaData;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableReferenceTraversal {

    private final TableReferenceMetaData tableReferenceMetaData;
    private final TableReferenceLoader tableReferenceLoader;

    public TableReferenceTraversal(TableReferenceMetaData tableReferenceMetaData, TableReferenceLoader tableReferenceLoader) {
        this.tableReferenceMetaData = tableReferenceMetaData;
        this.tableReferenceLoader = tableReferenceLoader;
    }

    public TableList traverseOutgoingReferences(ITable sourceTable) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        String tableName = tableMetaData.getTableName();

        try {
            TableReferenceList outgoingReferences = tableReferenceMetaData.getOutgoingReferences(tableName);
            return tableReferenceLoader.loadReferencedTables(sourceTable, outgoingReferences, TableReference.Direction.NATURAL);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

}
