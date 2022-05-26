package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import org.dbunit.dataset.ITable;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableBrowseSqlFactory {

    public SqlStatement createSqlStatement(BrowseTable tableBrowseRef);

    public SqlStatement createSqlStatement(BrowseTableReference targetTableBrowseRefeference, ITable sourceTable) throws Exception;
}
