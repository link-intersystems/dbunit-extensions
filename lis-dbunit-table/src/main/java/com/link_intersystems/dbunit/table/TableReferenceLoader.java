package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

import java.util.function.Predicate;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableReferenceLoader {
    TableList loadOutgoingReferences(ITable sourceTable) throws DataSetException;

    TableList loadOutgoingReferences(ITable sourceTable, Predicate<TableReference> referenceFilter) throws DataSetException;
}
