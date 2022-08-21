package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyExtension;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaTinyExtension
class SqlStatementTest {

    @Test
    void executeQuery(Connection connection) throws SQLException {
        SqlStatement sqlStatement = new SqlStatement("select * from actor where actor_id = ?", Arrays.asList(1));

        String firstName = sqlStatement.processResultSet(connection,
                rs -> {
                    rs.next();
                    return rs.getString("first_name");
                });

        assertEquals("PENELOPE", firstName);
    }

    @Test
    void testExecuteQuery() {
    }

    @Test
    void processResultSet() {
    }

    @Test
    void processQuery() {
    }
}