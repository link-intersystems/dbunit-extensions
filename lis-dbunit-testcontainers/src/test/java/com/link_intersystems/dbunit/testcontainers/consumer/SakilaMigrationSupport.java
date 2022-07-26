package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SakilaMigrationSupport implements DatabaseMigrationSupport {

    @Override
    public void prepareDataSource(DataSource dataSource) throws DataSetException {
        try {
            try (Connection connection = dataSource.getConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("create domain clob as text");
                statement.execute("create domain blob as bytea");
                new SakilaTinyDB().setupDdl(connection);
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void migrateDataSource(DataSource dataSource) throws DataSetException {

    }
}
