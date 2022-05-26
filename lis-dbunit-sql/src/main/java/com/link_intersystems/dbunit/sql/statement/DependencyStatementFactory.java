package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.ITable;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DependencyStatementFactory {

    public SqlStatement create(ITable sourceTable, TableReference.Edge sourceEdge, TableReference.Edge targetEdge) throws Exception;
}
