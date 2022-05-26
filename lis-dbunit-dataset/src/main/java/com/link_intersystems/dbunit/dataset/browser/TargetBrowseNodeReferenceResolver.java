package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dsl.BrowseTableReference;
import com.link_intersystems.dbunit.dsl.BrowseTable;
import com.link_intersystems.dbunit.meta.TableReference;
import com.link_intersystems.dbunit.meta.TableReferenceEdge;

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
            TableReferenceEdge sourceEdge = new TableReferenceEdge(sourceTableName, Arrays.asList(sourceColumns));
            TableReferenceEdge targetEdge = new TableReferenceEdge(targetTableName, Arrays.asList(targetColumns));
            String name = "user_defined(" + sourceTableName + "->" + targetTableName + ")";
            return new TableReference(name, sourceEdge, targetEdge);
        }

        return null;
    }
}
