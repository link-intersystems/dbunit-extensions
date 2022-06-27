package com.link_intersystems.dbunit.commands.exp;

import org.dbunit.dataset.DataSetException;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableOrder {
    String[] orderTables(String[] tableNames) throws DataSetException;
}
