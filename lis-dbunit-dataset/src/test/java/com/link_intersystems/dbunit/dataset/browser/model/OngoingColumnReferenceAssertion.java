package com.link_intersystems.dbunit.dataset.browser.model;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingColumnReferenceAssertion {
    private BrowseTable sourceTable;
    private String referencedTableName;

    OngoingColumnReferenceAssertion(BrowseTable sourceTable, String referencedTableName) {
        this.sourceTable = sourceTable;
        this.referencedTableName = referencedTableName;
    }

    public ColumnReferenceAssertion on(String... sourceColumns) {
        return new ColumnReferenceAssertion(sourceTable, referencedTableName, sourceColumns);
    }

}
