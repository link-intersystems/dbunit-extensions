package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.Test;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DBunitJdbcContainerTest {

    @Test
    void restart() throws DataSetException {
        DBunitJdbcContainer container = new DBunitJdbcContainer(DatabaseContainerSupport.getDatabaseContainerSupport("postgres"));

        RunningContainer runningContainer = container.start();
        runningContainer.stop();

        runningContainer = container.start();
        runningContainer.stop();
    }
}