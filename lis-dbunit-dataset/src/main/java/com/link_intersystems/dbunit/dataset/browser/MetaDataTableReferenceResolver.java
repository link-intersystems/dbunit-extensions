package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceException;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class MetaDataTableReferenceResolver implements TableReferenceResolver {

    private ConnectionMetaData connectionMetaData;

    MetaDataTableReferenceResolver(ConnectionMetaData connectionMetaData) {
        this.connectionMetaData = connectionMetaData;
    }

    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference targetBrowseNode) throws TableReferenceException {
        BrowseTable targetTableRef = targetBrowseNode.getTargetTableRef();
        String targetTableName = targetTableRef.getTableName();

        try {
            List<TableReference> outgoingDependencies = connectionMetaData.getOutgoingReferences(sourceTableName);
            TableReference targetTableReference = outgoingDependencies.stream().filter(d -> d.getTargetEdge().getTableName().equals(targetTableName)).findFirst().orElse(null);

            if (targetTableReference == null) {
                List<TableReference> incomingDependencies = connectionMetaData.getIncomingReferences(sourceTableName);
                targetTableReference = incomingDependencies.stream().filter(d -> d.getSourceEdge().getTableName().equals(sourceTableName)).findFirst().orElse(null);

                if (targetTableReference == null) {
                    String outgoingReferencesMsg = outgoingDependencies.stream().map(Object::toString).map(s -> "\n\t\t- " + s).collect(Collectors.joining());
                    String incomingReferencesMsg = incomingDependencies.stream().map(Object::toString).map(s -> "\n\t\t- " + s).collect(Collectors.joining());
                    String msg = format("No natural reference found from source table ''{0}'' to target table ''{1}''" +
                                    "\n\t- outgoing references: {2}" +
                                    "\n\t- incoming references: {3}",
                            sourceTableName,
                            targetTableName,
                            outgoingReferencesMsg,
                            incomingReferencesMsg);
                    throw new TableReferenceException(msg);
                }

            }

            return targetTableReference;
        } catch (SQLException e) {
            throw new TableReferenceException(e);
        }
    }

}
