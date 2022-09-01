package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DBunitJdbcContainerTest {

    @Test
    void restart() throws DataSetException {
        DBunitJdbcContainer container = new DBunitJdbcContainer(DatabaseContainerSupport.getDatabaseContainerSupport("postgres"));

        container.start();
        assertTrue(container.isRunning());
        container.stop();
        assertTrue(container.isStopped());

        container.start();
        assertTrue(container.isRunning());
        container.stop();
        assertTrue(container.isStopped());
    }
}