package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface SqlOperatorFormat {
    String getOperator();
    SqlOperator format(Object value);
}
