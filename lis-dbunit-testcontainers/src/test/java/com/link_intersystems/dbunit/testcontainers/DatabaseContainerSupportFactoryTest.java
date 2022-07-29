package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlMetadataHandler;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.dbunit.database.DatabaseConfig.PROPERTY_DATATYPE_FACTORY;
import static org.dbunit.database.DatabaseConfig.PROPERTY_METADATA_HANDLER;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseContainerSupportFactoryTest {

    public static class NonCompliantContainerApi extends PostgreSQLContainer<NonCompliantContainerApi> {

    }

    private DatabaseContainerSupportFactory supportFactory = DatabaseContainerSupportFactory.INSTANCE;

    @Test
    void wrongImageName() {
        DatabaseContainerSupport databaseContainerSupport = supportFactory.createPostgres("p:latest");
        assertThrows(IllegalStateException.class, () -> databaseContainerSupport.create());
    }

    @Test
    void testContainerWithNonCompliantApi() {
        String containerClass = "com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupportFactoryTest$NonCompliantContainerApi";
        assertThrows(IllegalStateException.class, () -> supportFactory.createContainerSupport(containerClass, "test:latest"));
    }


    @Test
    void testcontainersApiChangeSimulatedWithNonExistentContainerClass() {
        String changedApiSimulation = "org.testcontainers.containers.postgres.SQLContainer";
        assertThrows(IllegalStateException.class, () -> supportFactory.createContainerSupport(changedApiSimulation, "test:latest"));
    }

    @Test
    void forPostgres() {
        DatabaseContainerSupport databaseContainerSupport = supportFactory.createPostgres("postgres:latest");

        JdbcDatabaseContainer<?> jdbcDatabaseContainer = databaseContainerSupport.create();
        assertNotNull(jdbcDatabaseContainer);

        DatabaseConfig databaseConfig = databaseContainerSupport.getDatabaseConfig();

        IDataTypeFactory dataTypeFactory = (IDataTypeFactory) databaseConfig.getProperty(PROPERTY_DATATYPE_FACTORY);
        assertTrue(dataTypeFactory instanceof PostgresqlDataTypeFactory);
    }

    @Test
    void forMysql() {
        DatabaseContainerSupport databaseContainerSupport = supportFactory.createMysql("mysql:latest");

        JdbcDatabaseContainer<?> jdbcDatabaseContainer = databaseContainerSupport.create();
        assertNotNull(jdbcDatabaseContainer);

        DatabaseConfig databaseConfig = databaseContainerSupport.getDatabaseConfig();

        IMetadataHandler metaDataHandler = (IMetadataHandler) databaseConfig.getProperty(PROPERTY_METADATA_HANDLER);
        assertTrue(metaDataHandler instanceof MySqlMetadataHandler);

        IDataTypeFactory dataTypeFactory = (IDataTypeFactory) databaseConfig.getProperty(PROPERTY_DATATYPE_FACTORY);
        assertTrue(dataTypeFactory instanceof MySqlDataTypeFactory);
    }
}