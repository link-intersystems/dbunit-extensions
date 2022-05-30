package com.link_intersystems.dbunit.dataset.browser.model;

import org.junit.jupiter.api.Assertions;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTableAssertions {

    public static BrowseTableAssertions notNull(BrowseTable browseTable) {
        assertNotNull(browseTable);
        return new BrowseTableAssertions(browseTable);
    }

    private BrowseTable browseTable;

    private BrowseTableAssertions(BrowseTable browseTable) {
        this.browseTable = browseTable;
    }

    public BrowseTable getBrowseTable() {
        return browseTable;
    }

    public BrowseTableAssertions assertReferences(String referencedTableName) {
        List<BrowseTableReference> browseTableReferences = browseTable.getReferences();
        BrowseTable browseTable = browseTableReferences.stream()
                .filter(btr -> btr.getTargetBrowseTable().getTableName().equals(referencedTableName))
                .findFirst()
                .map(BrowseTableReference::getTargetBrowseTable)
                .orElse(null);
        assertNotNull(browseTable, referencedTableName);
        return new BrowseTableAssertions(browseTable);
    }

    public OngoingColumnReferenceAssertion assertReferencesByColumns(String referencedTableName) {
        return new OngoingColumnReferenceAssertion(browseTable, referencedTableName);

    }

    public void assertEmptyReferences() {
        Assertions.assertEquals(0, browseTable.getReferences().size(), browseTable.getTableName() + " empty references");
    }
}
