package com.link_intersystems.dbunit.dataset.browser.sql;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import org.dbunit.dataset.ITable;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BrowseTableSqlFactory {

    public SqlStatement selectSingleTable(BrowseTable browseTable);

    public SqlStatement selectReferencedTable(ITable sourceTable, BrowseTableReference targetTableReference) throws Exception;
}
