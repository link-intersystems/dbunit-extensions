package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceException;
import com.link_intersystems.jdbc.TableReferenceList;

import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class IncomingTableReferenceResolver implements TableReferenceResolver {

    private ConnectionMetaData connectionMetaData;

    public IncomingTableReferenceResolver(ConnectionMetaData connectionMetaData) {
        this.connectionMetaData = requireNonNull(connectionMetaData);
    }

    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference browseTableReference) throws TableReferenceException {
        BrowseTable targetBrowseTable = browseTableReference.getTargetBrowseTable();
        String targetTableName = targetBrowseTable.getTableName();

        try {
            TableReferenceList incomingDependencies = connectionMetaData.getIncomingReferences(sourceTableName);
            return incomingDependencies.stream()
                    .filter(tableReference -> tableReference.getSourceEdge().getTableName().equals(targetTableName))
                    .findFirst()
                    .map(TableReference::reverse)
                    .orElse(null);
        } catch (SQLException e) {
            throw new TableReferenceException(e);
        }
    }
}
