package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.TableReference;

import java.util.Arrays;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BrowseTableReferenceResolver implements TableReferenceResolver {
    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference browseTableReference) {
        BrowseTable targetBrowseTable = browseTableReference.getTargetBrowseTable();
        String targetTableName = targetBrowseTable.getTableName();

        String[] sourceColumns = browseTableReference.getSourceColumns();
        String[] targetColumns = browseTableReference.getTargetColumns();

        if (isValid(sourceColumns, targetColumns)) {
            TableReference.Edge sourceEdge = new TableReference.Edge(sourceTableName, Arrays.asList(sourceColumns));
            TableReference.Edge targetEdge = new TableReference.Edge(targetTableName, Arrays.asList(targetColumns));
            String name = "user_defined(" + sourceTableName + "->" + targetTableName + ")";
            return new TableReference(name, sourceEdge, targetEdge);
        }

        return null;
    }

    private boolean isValid(String[] sourceColumns, String[] targetColumns) {
        return (sourceColumns.length > 0 && targetColumns.length > 0) &&
                (sourceColumns.length == targetColumns.length);
    }
}
