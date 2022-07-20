package com.link_intersystems.dbunit.commands;

import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetTransformer {

    public IDataSetConsumer getInputConsumer();

    public void setOutputConsumer(IDataSetConsumer dataSetConsumer);
}
