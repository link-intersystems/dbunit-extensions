package com.link_intersystems.dbunit.testcontainers;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseContainerDataSourceTest {

    private static JdbcDatabaseContainer<?> databaseContainer;

    private DatabaseContainerDataSource dataSource;

    @BeforeAll
    static void beforeAll() {
        DatabaseContainerSupport postgres = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
        databaseContainer = postgres.create();
        databaseContainer.withUsername("user").withPassword("pass");

        databaseContainer.start();
    }

    @AfterAll
    static void afterAll() {
        databaseContainer.stop();
    }

    @BeforeEach
    void setUp() {
        dataSource = new DatabaseContainerDataSource(databaseContainer);
    }

    @AfterEach
    void tearDown() {
        dataSource.close();
    }

    @Test
    void getConnection() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("select 1");
            }
        }
    }

    @Test
    void connectionReuse() throws SQLException {
        Assertions.assertFalse(dataSource.isAutoCommit());

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
        }

        try (Connection connection = dataSource.getConnection()) {
            Assertions.assertTrue(connection.getAutoCommit());
        }
    }

    @Test
    void customConnection() throws SQLException {
        try (Connection connection = dataSource.getConnection("user", "pass")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("select 1");
            }
        }
    }

    @Test
    void reuseCustomConnection() throws SQLException {
        Assertions.assertFalse(dataSource.isAutoCommit());

        try (Connection connection = dataSource.getConnection("user", "pass")) {
            connection.setAutoCommit(true);
        }

        try (Connection connection = dataSource.getConnection("user", "pass")) {
            Assertions.assertTrue(connection.getAutoCommit());
        }
    }
}