package org.dbunit.database;

import org.dbunit.dataset.DataSetException;

/**
 * I need the functionality of DBUnit's {@link DatabaseTableMetaData} which only has package access.
 * Since I don't want to copy the code and I don't want to use reflection I created the access class here.
 * <p>
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 * @deprecated Even though this class is public, it is intended to be used by only classes of this library.
 */
@Deprecated
public class DatabaseTableMetaDataAccess extends DatabaseTableMetaData {

    public DatabaseTableMetaDataAccess(String tableName, IDatabaseConnection connection, boolean validate, boolean caseSensitiveMetaData) throws DataSetException {
        super(tableName, connection, validate, caseSensitiveMetaData);
    }

    public DatabaseTableMetaDataAccess(String tableName, IDatabaseConnection connection) throws DataSetException {
        super(tableName, connection);
    }

    public DatabaseTableMetaDataAccess(String tableName, IDatabaseConnection connection, boolean validate) throws DataSetException {
        super(tableName, connection, validate);
    }
}
