package com.link_intersystems.dbunit.testcontainers.postgres;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportProvider;
import com.link_intersystems.dbunit.testcontainers.DefaultDatabaseContainerSupport;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.dbunit.database.DatabaseConfig.PROPERTY_DATATYPE_FACTORY;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PostgresDatabaseContainerSupportProvider implements DatabaseContainerSupportProvider {

    @Override
    public boolean canProvideSupport(DockerImageName dockerImageName) {
        return dockerImageName.getUnversionedPart().contains("postgres");
    }

    @Override
    public DatabaseContainerSupport createDatabaseContainerSupport(DockerImageName dockerImageName) {
        DefaultDatabaseContainerSupport containerSupport = new DefaultDatabaseContainerSupport(() -> new PostgreSQLContainer<>(dockerImageName));
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
        return containerSupport;
    }
}
