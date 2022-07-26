package com.link_intersystems.dbunit.testcontainers.consumer;

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

    public static final DatabaseContainerSupportFactory INSTANCE = new DatabaseContainerSupportFactory();

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
    public DatabaseContainerSupport createPostgres(String dockerImageName) {
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
    public DatabaseContainerSupport createMysql(String dockerImageName) {
        String containerClass = "org.testcontainers.containers.MySQLContainer";
        DefaultDatabaseContainerSupport containerSupport = createContainerSupport(containerClass, dockerImageName);
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_DATATYPE_FACTORY, new MySqlDataTypeFactory());
        containerSupport.getDatabaseConfig().setProperty(PROPERTY_METADATA_HANDLER, new MySqlMetadataHandler());
        return containerSupport;
    }

    protected DefaultDatabaseContainerSupport createContainerSupport(String containerClassName, String dockerImageName) {
        try {
            Class<?> containerClass = Class.forName(containerClassName);
            try {
                Constructor<?> containerConstructor = containerClass.getDeclaredConstructor(String.class);
                return instantiateContainer(containerConstructor, dockerImageName);
            } catch (NoSuchMethodException e) {
                throw containerImplementationConstructorNotFound(containerClassName, e);
            }
        } catch (ClassNotFoundException e) {
            throw noContainerImplementationFound(containerClassName, e);
        }
    }

    protected DefaultDatabaseContainerSupport instantiateContainer(Constructor<?> containerConstructor, Object dockerImageName) {
        return new DefaultDatabaseContainerSupport(() -> {
            try {
                return (JdbcDatabaseContainer<?>) containerConstructor.newInstance(dockerImageName);
            } catch (InvocationTargetException e) {
                throw handleInvocationTargetException(e);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });
    }


    protected RuntimeException handleInvocationTargetException(InvocationTargetException e) {
        Throwable targetException = e.getTargetException();

        if (targetException instanceof RuntimeException) {
            return (RuntimeException) targetException;
        }

        return new RuntimeException(targetException);
    }

    private IllegalStateException noContainerImplementationFound(String containerClass, Throwable e) {
        String className = DatabaseContainerSupport.class.getSimpleName();
        String msg = format("Can not create {0}. Implementation class ''{1}'' not found. " +
                        "Is the testcontainers library on the classpath? " +
                        "Otherwise the testcontainers library is not compatible and you have to implement a custom {2}.",
                className,
                containerClass,
                DatabaseContainerSupport.class
        );
        return new IllegalStateException(msg, e);
    }

    private IllegalStateException containerImplementationConstructorNotFound(String containerClass, Throwable e) {
        String className = DatabaseContainerSupport.class.getSimpleName();
        String msg = format("Can not create {0}. Missing constructor ''{1}(String)''.",
                className,
                containerClass
        );
        return new IllegalStateException(msg, e);
    }
}
