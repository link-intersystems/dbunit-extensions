package com.link_intersystems.dbunit.dataset.browser.resolve;

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
public class OutgoingTableReferenceResolver implements TableReferenceResolver {

    private static class OutgoingBrowseableReferencePredicate implements Predicate<TableReference> {

        private BrowseTableReference browseTableReference;

        public OutgoingBrowseableReferencePredicate(BrowseTableReference browseTableReference) {
            this.browseTableReference = browseTableReference;
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

    private TableReferenceMetaData tableReferenceMetaData;

    public OutgoingTableReferenceResolver(TableReferenceMetaData tableReferenceMetaData) {
        this.tableReferenceMetaData = requireNonNull(tableReferenceMetaData);
    }

    @Override
    public TableReference getTableReference(String sourceTableName, BrowseTableReference browseTableReference) throws TableReferenceException {
        try {
            OutgoingBrowseableReferencePredicate predicate = new OutgoingBrowseableReferencePredicate(browseTableReference);

            Stream<TableReference> tableReferences = getTableReferences(tableReferenceMetaData, sourceTableName);
            return tableReferences
                    .filter(predicate)
                    .findFirst()
                    .orElse(null);
        } catch (SQLException e) {
            throw new TableReferenceException(e);
        }
    }

    protected Stream<TableReference> getTableReferences(TableReferenceMetaData tableReferenceMetaData, String tableName) throws SQLException {
        TableReferenceList outgoingDependencies = tableReferenceMetaData.getOutgoingReferences(tableName);
        return outgoingDependencies.stream();
    }

}
