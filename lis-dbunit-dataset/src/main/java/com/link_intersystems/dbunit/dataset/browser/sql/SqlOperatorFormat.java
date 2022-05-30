package com.link_intersystems.dbunit.dataset.browser.sql;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface SqlOperatorFormat {
    String getOperator();
    SqlOperator format(Object value);
}
