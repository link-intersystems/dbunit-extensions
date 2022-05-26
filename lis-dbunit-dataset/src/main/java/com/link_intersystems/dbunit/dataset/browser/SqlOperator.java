package com.link_intersystems.dbunit.dataset.browser;

import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class SqlOperator {
    private String sql;
    private List<Object> arguments;

    public SqlOperator(String sql, Object argument) {
        this(sql, Arrays.asList(argument));
    }

    public SqlOperator(String sql, List<Object> arguments) {
        this.sql = sql;
        this.arguments = arguments;
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getArguments() {
        return arguments;
    }
}
