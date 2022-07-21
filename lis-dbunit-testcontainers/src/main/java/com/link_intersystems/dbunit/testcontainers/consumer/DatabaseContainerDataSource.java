package com.link_intersystems.dbunit.testcontainers.consumer;

import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseContainerDataSource extends AbstractDataSource {

    private JdbcDatabaseContainer<?> databaseContainer;

    private Connection sharedDefaultConnection;
    private Map<HashedCredentials, Connection> customConnections = new HashMap<>();

    private boolean autoCommit;

    public DatabaseContainerDataSource(JdbcDatabaseContainer<?> databaseContainer) {
        this.databaseContainer = requireNonNull(databaseContainer);
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (sharedDefaultConnection == null) {
            String username = databaseContainer.getUsername();
            String password = databaseContainer.getPassword();
            sharedDefaultConnection = createConnection(username, password);
            sharedDefaultConnection.setAutoCommit(autoCommit);
        }

        return ReusableConnectionProxy.createProxy(sharedDefaultConnection);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        HashedCredentials hashedCredentials = new HashedCredentials(username, password);

        Connection connection = customConnections.get(hashedCredentials);
        if (connection == null) {
            connection = createConnection(username, password);
            customConnections.put(hashedCredentials, connection);
        }

        return ReusableConnectionProxy.createProxy(connection);
    }

    private Connection createConnection(String username, String password) throws SQLException {
        String jdbcUrl = databaseContainer.getJdbcUrl();
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    public void close() {
        if (sharedDefaultConnection != null) {
            try {
                sharedDefaultConnection.close();
                sharedDefaultConnection = null;
            } catch (SQLException e) {
            }
        }

        for (Connection connection : customConnections.values()) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }

        customConnections.clear();
    }


}
