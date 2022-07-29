package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CompositeDataSetException extends DataSetException {

    private List<DataSetException> dataSetExceptions = new ArrayList<>();

    public CompositeDataSetException(List<DataSetException> dataSetExceptions) {
        this.dataSetExceptions.addAll(dataSetExceptions);
    }

    public List<DataSetException> getDataSetExceptions() {
        return dataSetExceptions;
    }
}
