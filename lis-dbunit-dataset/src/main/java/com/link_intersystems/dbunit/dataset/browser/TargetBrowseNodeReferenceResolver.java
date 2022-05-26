package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.jdbc.TableReference;

import java.util.Arrays;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TargetBrowseNodeReferenceResolver implements TableReferenceResolver {
    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference targetBrowseNode) {
        BrowseTable targetTableRef = targetBrowseNode.getTargetTableRef();
        String targetTableName = targetTableRef.getTableName();

        String[] sourceColumns = targetBrowseNode.getSourceColumns();
        String[] targetColumns = targetBrowseNode.getTargetColumns();

        if (sourceColumns != null && targetColumns != null) {
            TableReference.Edge sourceEdge = new TableReference.Edge(sourceTableName, Arrays.asList(sourceColumns));
            TableReference.Edge targetEdge = new TableReference.Edge(targetTableName, Arrays.asList(targetColumns));
            String name = "user_defined(" + sourceTableName + "->" + targetTableName + ")";
            return new TableReference(name, sourceEdge, targetEdge);
        }

        return null;
    }
}
