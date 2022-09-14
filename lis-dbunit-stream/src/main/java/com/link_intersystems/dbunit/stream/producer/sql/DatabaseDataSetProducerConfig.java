package com.link_intersystems.dbunit.stream.producer.sql;

import org.dbunit.dataset.filter.ITableFilterSimple;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetProducerConfig {

    private ITableFilterSimple tableFilter = t -> true;
    private String schema;
    private boolean caseSensitiveTableNames = false;

    public void setTableFilter(ITableFilterSimple tableFilter) {
        this.tableFilter = requireNonNull(tableFilter);
    }

    public ITableFilterSimple getTableFilter() {
        return tableFilter;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public boolean isCaseSensitiveTableNames() {
        return caseSensitiveTableNames;
    }

    public void setCaseSensitiveTableNames(boolean caseSensitiveTableNames) {
        this.caseSensitiveTableNames = caseSensitiveTableNames;
    }
}
