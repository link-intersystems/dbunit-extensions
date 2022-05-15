package com.link_intersystems.dbunit.statement;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DependencyStatementFactory {

    public SqlStatement create(DatabaseConfig config, ITable sourceTable, Dependency dependency) throws DataSetException;
}
