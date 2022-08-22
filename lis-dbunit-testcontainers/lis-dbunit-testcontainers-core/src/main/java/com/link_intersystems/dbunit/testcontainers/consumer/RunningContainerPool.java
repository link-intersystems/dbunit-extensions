package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface RunningContainerPool extends AutoCloseable {

    public RunningContainer borrowContainer() throws DataSetException;

    public void returnContainer(RunningContainer runningContainer);
}
