package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetSupplier {

    public IDataSet get() throws DataSetException;
}
