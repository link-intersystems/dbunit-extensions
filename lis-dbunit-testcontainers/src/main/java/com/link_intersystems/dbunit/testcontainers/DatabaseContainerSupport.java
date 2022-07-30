package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.database.DatabaseConfig;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ServiceLoader;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class DatabaseContainerSupport {

    /**
     * @return a {@link DatabaseContainerSupport} for the provided dockerImageName if available.
     * Support can be added through META-INF/services/com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportProvider.
     * For details take a look at the javadoc of java.util.ServiceLoader.
     */
    public static DatabaseContainerSupport getDatabaseContainerSupport(String dockerImageName) {
        return getDatabaseContainerSupport(DockerImageName.parse(dockerImageName));
    }

    /**
     * @return a {@link DatabaseContainerSupport} for the provided dockerImageName if available.
     * Support can be added through META-INF/services/com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportProvider.
     * For details take a look at the javadoc of java.util.ServiceLoader.
     */
    public static DatabaseContainerSupport getDatabaseContainerSupport(DockerImageName dockerImageName) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        classLoader = classLoader == null ? DatabaseContainerSupport.class.getClassLoader() : classLoader;

        ServiceLoader<DatabaseContainerSupportProvider> providersLoader = ServiceLoader.load(DatabaseContainerSupportProvider.class, classLoader);
        for (DatabaseContainerSupportProvider supportProvider : providersLoader) {
            if (supportProvider.canProvideSupport(dockerImageName)) {
                return supportProvider.createDatabaseContainerSupport(dockerImageName);
            }
        }
        StringBuilder msg = new StringBuilder();
        msg.append("No ");
        msg.append(DatabaseContainerSupportProvider.class.getName());
        msg.append(" available for docker image '");
        msg.append(dockerImageName);
        msg.append("'.\n");
        msg.append("\tYou can implement your own provider, put it on the classpath and make it available through a\n");
        msg.append("\tMETA-INF/services/com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportProvider entry.\n");
        msg.append("\tFor details take a look at the javadoc of ");
        msg.append(ServiceLoader.class.getName());

        throw new IllegalStateException(msg.toString());
    }

    public abstract JdbcDatabaseContainer<?> create();

    public DatabaseConfig getDatabaseConfig() {
        return new DatabaseConfig();
    }
}
