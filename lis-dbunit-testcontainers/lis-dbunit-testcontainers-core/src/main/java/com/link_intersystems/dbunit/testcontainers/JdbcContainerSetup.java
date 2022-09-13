package com.link_intersystems.dbunit.testcontainers;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface JdbcContainerSetup {

    public void setup(JdbcContainer jdbcContainer) throws Exception;
}
