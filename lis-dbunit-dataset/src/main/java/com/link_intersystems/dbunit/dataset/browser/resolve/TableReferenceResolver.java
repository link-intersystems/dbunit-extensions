package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableReferenceResolver {

    public TableReference getTableReference(String sourceTableName, BrowseTableReference targetBrowseNode) throws TableReferenceException;
}
