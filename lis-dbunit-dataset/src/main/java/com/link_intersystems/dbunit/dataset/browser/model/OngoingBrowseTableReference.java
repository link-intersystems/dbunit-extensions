package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class OngoingBrowseTableReference {
    private BrowseTableReference browseTableReference;

    public OngoingBrowseTableReference(BrowseTableReference browseTableReference) {
        this.browseTableReference = browseTableReference;
    }

    public BrowseTable references(String... targetColumns) {
        browseTableReference.setTargetColumns(targetColumns);
        return browseTableReference.getTargetBrowseTable();
    }

    public BrowseTable references(List<String> targetColumns) {
        return references(targetColumns.toArray(new String[0]));
    }
}
