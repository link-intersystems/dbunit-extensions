package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.database.IDatabaseConnection;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface RunningContainer {
    DataSource getDataSource();

    IDatabaseConnection getDatabaseConnection();

    void stop();
}
