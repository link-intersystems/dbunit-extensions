package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class IncomingTableReferenceResolver extends OutgoingTableReferenceResolver {

    public IncomingTableReferenceResolver(TableReferenceMetaData tableReferenceMetaData) {
        super(tableReferenceMetaData);
    }

    @Override
    protected Stream<TableReference> getTableReferences(TableReferenceMetaData tableReferenceMetaData, String tableName) throws SQLException {
        TableReferenceList incomingReferences = tableReferenceMetaData.getIncomingReferences(tableName);
        return incomingReferences.stream().map(TableReference::reverse);
    }
}
