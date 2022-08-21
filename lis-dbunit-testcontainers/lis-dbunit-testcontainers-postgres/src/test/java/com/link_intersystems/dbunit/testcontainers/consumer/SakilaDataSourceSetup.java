package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SakilaDataSourceSetup {

    public void prepareDataSource(DataSource dataSource) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("create domain clob as text");
            statement.execute("create domain blob as bytea");
            new SakilaTinyDB().setupDdl(connection);
        }
    }
}
