package com.link_intersystems.dbunit.stream.resource;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetResource {
    IDataSetProducer createProducer() throws DataSetException;

    IDataSetConsumer createConsumer() throws DataSetException;

    default <T> T getAdapter(Class<T> adapterType) {
        if (adapterType.isInstance(this)) {
            return adapterType.cast(this);
        }
        return null;
    }
}
