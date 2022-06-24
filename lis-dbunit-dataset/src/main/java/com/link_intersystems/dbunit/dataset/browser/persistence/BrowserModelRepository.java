package com.link_intersystems.dbunit.dataset.browser.persistence;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BrowserModelRepository {

    public void persistModel(BrowseTable browseTable, String name) throws ModelPersistenceException;

    public BrowseTable loadModel(String name) throws ModelPersistenceException;

}
