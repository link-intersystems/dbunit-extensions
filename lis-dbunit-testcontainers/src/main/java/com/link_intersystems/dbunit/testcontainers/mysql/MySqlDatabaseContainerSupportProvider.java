package com.link_intersystems.dbunit.testcontainers.mysql;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportProvider;
import com.link_intersystems.dbunit.testcontainers.DefaultDatabaseContainerSupport;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlMetadataHandler;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.dbunit.database.DatabaseConfig.PROPERTY_DATATYPE_FACTORY;
import static org.dbunit.database.DatabaseConfig.PROPERTY_METADATA_HANDLER;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MySqlDatabaseContainerSupportProvider implements DatabaseContainerSupportProvider {

    @Override
    public boolean canProvideSupport(DockerImageName dockerImageName) {
        return dockerImageName.getUnversionedPart().contains("mysql");
    }

    @Override
    public DatabaseContainerSupport createDatabaseContainerSupport(DockerImageName dockerImageName) {
        DefaultDatabaseContainerSupport containerSupport = new DefaultDatabaseContainerSupport(() -> new MySQLContainer<>(dockerImageName));
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_DATATYPE_FACTORY, new MySqlDataTypeFactory());
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
        return containerSupport;
    }
}
