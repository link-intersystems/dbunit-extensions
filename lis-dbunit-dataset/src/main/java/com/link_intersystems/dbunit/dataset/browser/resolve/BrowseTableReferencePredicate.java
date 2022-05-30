package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.TableReference;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTableReferencePredicate implements Predicate<TableReference> {

    private BrowseTableReference browseTableReference;
    private Function<TableReference, TableReference.Edge> edgeGetter;

    public BrowseTableReferencePredicate(BrowseTableReference browseTableReference, Function<TableReference, TableReference.Edge> edgeGetter) {
        this.browseTableReference = browseTableReference;
        this.edgeGetter = edgeGetter;
    }

    @Override
    public boolean test(TableReference tableReference) {
        TableReference.Edge targetEdge = tableReference.getTargetEdge();

        String targetTableName = targetEdge.getTableName();
        if (!targetTableName.equals(browseTableReference.getTargetBrowseTable().getTableName())) {
            return false;
        }

        String[] sourceColumns = browseTableReference.getSourceColumns();
        if (sourceColumns.length > 0) {
            TableReference.Edge sourceEdge = tableReference.getSourceEdge();
            List<String> sourceEdgeColumns = sourceEdge.getColumns();
            if (!Arrays.asList(sourceColumns).equals(sourceEdgeColumns)) {
                return false;
            }
        }

        String[] targetColumns = browseTableReference.getTargetColumns();
        if (targetColumns.length > 0) {
            List<String> targetEdgeColumns = targetEdge.getColumns();
            if (!Arrays.asList(targetColumns).equals(targetEdgeColumns)) {
                return false;
            }
        }

        return true;
    }
}
