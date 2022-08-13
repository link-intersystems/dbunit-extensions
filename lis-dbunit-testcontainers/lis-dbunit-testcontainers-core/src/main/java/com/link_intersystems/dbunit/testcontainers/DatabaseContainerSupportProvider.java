package com.link_intersystems.dbunit.testcontainers;

import org.testcontainers.utility.DockerImageName;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseContainerSupportProvider {

    public boolean canProvideSupport(DockerImageName dockerImageName);

    public DatabaseContainerSupport createDatabaseContainerSupport(DockerImageName dockerImageName);
}
