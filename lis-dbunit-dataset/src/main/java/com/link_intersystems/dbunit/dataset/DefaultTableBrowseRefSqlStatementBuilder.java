package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultTableBrowseRefSqlStatementBuilder implements TableBrowseRefSqlStatementBuilder {

    private TableMetaDataRepository tableMetaDataRepository;

    public DefaultTableBrowseRefSqlStatementBuilder(TableMetaDataRepository tableMetaDataRepository) {
        this.tableMetaDataRepository = tableMetaDataRepository;
    }

    @Override
    public SqlStatement createSqlStatement(TableBrowseRef tableBrowseRef) {
        SelectTableRefBuilder selectTableRefBuilder = new SelectTableRefBuilder(tableBrowseRef);
        return selectTableRefBuilder.toSqlStatement();
    }
}
