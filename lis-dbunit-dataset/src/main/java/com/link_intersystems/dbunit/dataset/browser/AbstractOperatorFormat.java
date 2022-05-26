package com.link_intersystems.dbunit.dataset.browser;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractOperatorFormat implements SqlOperatorFormat {
    private String operator;

    protected AbstractOperatorFormat(String operator) {
        this.operator = requireNonNull(operator).trim();
        if (this.operator.isEmpty()) {
            String msg = "Operator must not be blank";
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public String getOperator() {
        return operator;
    }
}
