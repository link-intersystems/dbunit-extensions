package com.link_intersystems.dbunit.testcontainers.consumer;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.ext.mysql.MySqlDataTypeFactory;
import org.dbunit.ext.mysql.MySqlMetadataHandler;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static java.text.MessageFormat.format;
import static org.dbunit.database.DatabaseConfig.PROPERTY_DATATYPE_FACTORY;
import static org.dbunit.database.DatabaseConfig.PROPERTY_METADATA_HANDLER;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseContainerSupportFactory {

    /**
     * Ensure that the testcontainers postgres library is on the classpath. E.g.
     * <pre>
     * &lt;dependency>
     *     &lt;groupId>org.testcontainers&lt;/groupId>
     *     &lt;artifactId>postgresql&lt;/artifactId>
     *     &lt;version>1.17.3&lt;/version>
     * &lt;/dependency>
     * </pre>
     *
     * @param dockerImageName the docker image name, e.g. "postgres:latest".
     */
    public static DatabaseContainerSupport forPostgres(String dockerImageName) {
        String containerClass = "org.testcontainers.containers.PostgreSQLContainer";
        DefaultDatabaseContainerSupport containerSupport = createContainerSupport(containerClass, dockerImageName);
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
        return containerSupport;

    }



    /**
     * Ensure that the testcontainers mysql library is on the classpath as well as the mysql jdbc driver. E.g.
     *
     * <pre>
     * &lt;dependency>
     *     &lt;groupId>org.testcontainers&lt;/groupId>
     *     &lt;artifactId>mysql&lt;/artifactId>
     *     &lt;version>1.17.2&lt;/version>
     * &lt;/dependency>
     * &lt;dependency>
     *     &lt;groupId>mysql&lt;/groupId>
     *     &lt;artifactId>mysql-connector-java&lt;/artifactId>
     *     &lt;version>8.0.29&lt;/version>
     * &lt;/dependency>
     * </pre>
     *
     * @param dockerImageName the docker image name, e.g. "postgres:latest".
     */
    public static DatabaseContainerSupport forMysql(String dockerImageName) {
        String containerClass = "org.testcontainers.containers.MySQLContainer";
        DefaultDatabaseContainerSupport containerSupport = createContainerSupport(containerClass, dockerImageName);
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_DATATYPE_FACTORY, new MySqlDataTypeFactory());
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
        return containerSupport;
    }

    private static DefaultDatabaseContainerSupport createContainerSupport(String containerClass, String dockerImageName) {
        try {
            Class<?> postgresContainerClass = Class.forName(containerClass);
            Constructor<?> containerConstructor = postgresContainerClass.getDeclaredConstructor(String.class);
            return new DefaultDatabaseContainerSupport(() -> {
                try {
                    return (JdbcDatabaseContainer<?>) containerConstructor.newInstance(dockerImageName);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (NoClassDefFoundError | ClassNotFoundException | NoSuchMethodException e) {
            throw noContainerSuppportAvailable("postgres", e);
        }
    }

    private static IllegalStateException noContainerSuppportAvailable(String type, Throwable e) {
        String className = DatabaseContainerSupport.class.getSimpleName();
        String msg = format("Can not create {0} of type {1}. Is the testcontainers {1} library on the classpath?", className, type);
        return new IllegalStateException(msg, e);
    }
}
