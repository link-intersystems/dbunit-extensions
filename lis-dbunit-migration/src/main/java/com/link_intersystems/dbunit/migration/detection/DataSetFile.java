package com.link_intersystems.dbunit.migration.detection;

import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFile {

    public IDataSetProducer createProducer();

    public IDataSetConsumer createConsumer();
}
