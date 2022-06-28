package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.statement.InsertSql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    public static interface ResultSetMapper<T> {
        public T apply(ResultSet rs) throws Exception;
    }

    private CharSequence sql;
    private List<Object> arguments = new ArrayList<>();

    public SqlStatement(CharSequence sql, List<Object> arguments) {
        this.sql = sql;
        this.arguments.addAll(arguments);
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public CharSequence getSql() {
        return sql;
    }

    public void executeQuery(Connection connection, PreparedStatementConsumer preparedStatementConsumer) throws Exception {
        executeQuery(connection, (PreparedStatementFunction<Void>) preparedStatement -> {
            preparedStatementConsumer.accept(preparedStatement);
            return null;
        });
    }

    public <T> T executeQuery(Connection connection, PreparedStatementFunction<T> preparedStatementConsumer) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {


            int paramIndex = 1;

            for (Object argument : arguments) {
                ps.setObject(paramIndex++, argument);
            }

            try {
                return preparedStatementConsumer.apply(ps);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    public <T> T processResultSet(Connection connection, ResultSetMapper<T> resultSetMapper) throws SQLException {
        T queryResults = executeQuery(connection, ps -> {

            if (ps.execute()) {
                ResultSet rs = ps.getResultSet();
                return resultSetMapper.apply(rs);
            }

            return null;
        });

        return queryResults;
    }

    public <T> List<T> processQuery(Connection connection, ResultSetMapper<T> resultSetMapper) throws SQLException {


        List<T> queryResults = processResultSet(connection, rs -> {
            List<T> results = new ArrayList<>();

            while (rs.next()) {
                T result = resultSetMapper.apply(rs);
                results.add(result);
            }

            return results;
        });

        return queryResults;
    }
}
