package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceException;
import com.link_intersystems.jdbc.TableReferenceList;

import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OutgoingTableReferenceResolver implements TableReferenceResolver {

    private ConnectionMetaData connectionMetaData;

    public OutgoingTableReferenceResolver(ConnectionMetaData connectionMetaData) {
        this.connectionMetaData = requireNonNull(connectionMetaData);
    }

    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference browseTableReference) throws TableReferenceException {
        BrowseTable targetBrowseTable = browseTableReference.getTargetBrowseTable();
        String targetTableName = targetBrowseTable.getTableName();
        try {
            TableReferenceList outgoingDependencies = connectionMetaData.getOutgoingReferences(sourceTableName);
            return outgoingDependencies.stream()
                    .filter(tableReference -> tableReference.getTargetEdge().getTableName().equals(targetTableName))
                    .findFirst()
                    .orElse(null);
        } catch (SQLException e) {
            throw new TableReferenceException(e);
        }
    }
}
