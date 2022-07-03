package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceException;
import com.link_intersystems.jdbc.TableReferenceList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;

/**
 * A {@link TableReferenceResolver} that first tries to resolve {@link TableReference}s
 * by a {@link com.link_intersystems.dbunit.dataset.browser.model.BrowseTable}
 * definition, then by the database table's outgoing references
 * and last by the database table's incoming references.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultTableReferenceResolverChain implements TableReferenceResolver {

    private List<TableReferenceResolver> chain;
    private ConnectionMetaData connectionMetaData;

    public DefaultTableReferenceResolverChain(ConnectionMetaData connectionMetaData) {
        this.connectionMetaData = connectionMetaData;
    }

    private List<TableReferenceResolver> getChain() {
        if (chain == null) {
            chain = initChain(connectionMetaData);
        }
        return chain;
    }

    protected List<TableReferenceResolver> initChain(ConnectionMetaData connectionMetaData) {
        List<TableReferenceResolver> chain = new ArrayList<>();

        chain.add(new BrowseTableReferenceResolver());
        chain.add(new OutgoingTableReferenceResolver(connectionMetaData));
        chain.add(new IncomingTableReferenceResolver(connectionMetaData));

        return chain;
    }

    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference targetBrowseNode) throws TableReferenceException {
        TableReference tableReference = null;

        Iterator<TableReferenceResolver> iterator = getChain().iterator();
        while (tableReference == null && iterator.hasNext()) {
            TableReferenceResolver referenceResolver = iterator.next();
            tableReference = referenceResolver.getTableReference(sourceTableName, targetBrowseNode);
        }

        if (tableReference == null) {
            String targetTableName = targetBrowseNode.getTargetBrowseTable().getTableName();
            handleNoReferenceFound(sourceTableName, targetTableName);
        }


        return tableReference;
    }

    protected void handleNoReferenceFound(String sourceTableName, String targetTableName) throws TableReferenceException {
        try {
            tryHandleNoReferenceFound(sourceTableName, targetTableName);
        } catch (SQLException e) {
            String msg = format("No natural reference found from source table ''{0}'' to target table ''{1}''",
                    sourceTableName,
                    targetTableName);
            TableReferenceException tableReferenceException = new TableReferenceException(msg);
            tableReferenceException.initCause(e);
            throw tableReferenceException;
        }
    }

    protected void tryHandleNoReferenceFound(String sourceTableName, String targetTableName) throws SQLException {
        TableReferenceList outgoingDependencies = connectionMetaData.getOutgoingReferences(sourceTableName);
        TableReferenceList incomingDependencies = connectionMetaData.getIncomingReferences(sourceTableName);
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
