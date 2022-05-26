package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class LikeOperatorFormat implements SqlOperatorFormat {
    @Override
    public SqlOperator format(Object value) {
        return new SqlOperator("LIKE ?", value);
    }
}
