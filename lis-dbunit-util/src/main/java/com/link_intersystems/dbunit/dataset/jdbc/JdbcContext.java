package com.link_intersystems.dbunit.dataset.jdbc;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcContext {
    private String catalog;
    private String schema;

    public JdbcContext(String catalog, String schema) {
        this.catalog = catalog;
        this.schema = schema;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }
}
