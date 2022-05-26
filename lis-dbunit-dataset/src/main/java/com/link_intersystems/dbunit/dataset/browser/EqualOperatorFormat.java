package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EqualOperatorFormat implements SqlOperatorFormat {
    @Override
    public SqlOperator format(Object value) {
        return new SqlOperator("= ?", value);
    }
}
