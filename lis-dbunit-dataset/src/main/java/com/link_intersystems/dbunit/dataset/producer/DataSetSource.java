package com.link_intersystems.dbunit.dataset.producer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetSource {

    public IDataSet get() throws DataSetException;
}
