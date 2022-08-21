package com.link_intersystems.dbunit.migration;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseMigrationSupport {
    void prepareDataSource(DataSource dataSource) throws SQLException;

    void migrateDataSource(DataSource dataSource) throws SQLException;

}
