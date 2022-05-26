package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.dbunit.meta.TableReferenceEdge;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DependencyStatementFactory {

    public SqlStatement create(ITable sourceTable, TableReferenceEdge sourceEdge, TableReferenceEdge targetEdge) throws Exception;
}
