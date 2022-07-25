package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetConsumerPipe extends IDataSetConsumer {

    public void setSubsequentConsumer(IDataSetConsumer dataSetConsumer);

}
