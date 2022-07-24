package com.link_intersystems.dbunit.migration.detection;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFile {

    public IDataSetProducer createProducer() throws DataSetException;

    public IDataSetConsumer createConsumer() throws DataSetException;

    DataSetFile withNewPath(Path path) throws DataSetException;
}
