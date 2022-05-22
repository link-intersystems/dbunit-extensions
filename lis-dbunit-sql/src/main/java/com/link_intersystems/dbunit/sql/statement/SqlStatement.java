package com.link_intersystems.dbunit.sql.statement;

import org.dbunit.dataset.DataSetException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SqlStatement {

    @FunctionalInterface
    public static interface PreparedStatementConsumer {
        public void accept(PreparedStatement ps) throws Exception;
    }

    public static interface PreparedStatementFunction<T> {
        public T apply(PreparedStatement ps) throws Exception;
    }

    private CharSequence sql;
    private List<Object> arguments = new ArrayList<>();

    public SqlStatement(CharSequence sql, List<Object> arguments) {
        this.sql = sql;
        this.arguments.addAll(arguments);
    }

    public void executeQuery(Connection connection, PreparedStatementConsumer preparedStatementConsumer) throws Exception {
        executeQuery(connection, (PreparedStatementFunction<Void>) preparedStatement -> {
            preparedStatementConsumer.accept(preparedStatement);
            return null;
        });
    }

    public <T> T executeQuery(Connection connection, PreparedStatementFunction<T> preparedStatementConsumer) throws Exception {
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {


            int paramIndex = 1;

            for (Object argument : arguments) {
                ps.setObject(paramIndex++, argument);
            }

            return preparedStatementConsumer.apply(ps);
        }
    }
}
