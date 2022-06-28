package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableReferenceLoader {

    default public TableList loadReferencedTables(ITable sourceTable, List<TableReference> references, TableReference.Direction direction) throws DataSetException {
        TableList tables = new TableList();

        for (TableReference reference : references) {
            ITable referencedTable = loadReferencedTable(sourceTable, reference, direction);
            tables.add(referencedTable);
        }

        return tables;
    }

    ITable loadReferencedTable(ITable sourceTable, TableReference tableReference, TableReference.Direction direction) throws DataSetException;
}
