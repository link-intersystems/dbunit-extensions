package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;

import java.sql.PreparedStatement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableBrowseRefSqlStatementBuilder {

    public SqlStatement createSqlStatement(TableBrowseRef tableBrowseRef);
}
