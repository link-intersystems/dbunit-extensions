package com.link_intersystems.dbunit.dataset.browser;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SingleValueOperatorFormat extends AbstractOperatorFormat {

    public SingleValueOperatorFormat(String operator) {
        super(operator);
    }

    @Override
    public SqlOperator format(Object value) {
        return new SqlOperator(getOperator() + " ?", value);
    }
}
