package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dsl.BrowseTableReference;
import com.link_intersystems.dbunit.meta.TableReference;
import com.link_intersystems.dbunit.meta.TableReferenceException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableReferenceResolver {

    public TableReference getTableReference(String sourceTableName, BrowseTableReference targetBrowseNode) throws TableReferenceException;
}
