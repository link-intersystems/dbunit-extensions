package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingBrowseTable {

    private BrowseTableReference browseTableReference;

    OngoingBrowseTable(BrowseTableReference browseTableReference) {
        this.browseTableReference = browseTableReference;
    }

    public BrowseTable natural() {
        return browseTableReference.getTargetBrowseTable();
    }

    public OngoingBrowseTableReference on(String... sourceColumnNames) {
        browseTableReference.setSourceColumns(sourceColumnNames);
        return new OngoingBrowseTableReference(browseTableReference);
    }

    public OngoingBrowseTableReference on(List<String> sourceColumnNames) {
        return on(sourceColumnNames.toArray(new String[0]));
    }
}
