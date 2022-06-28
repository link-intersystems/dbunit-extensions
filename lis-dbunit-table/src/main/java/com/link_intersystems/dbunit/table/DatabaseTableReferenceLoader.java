package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.JoinTableReferenceSqlFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.dbunit.sql.statement.TableReferenceSqlFactory;
import com.link_intersystems.jdbc.TableReference;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseTableReferenceLoader implements TableReferenceLoader {

    private final IDatabaseConnection databaseConnection;
    private final TableReferenceSqlFactory tableReferenceSqlFactory;
    private final TableMetaDataRepository tableMetaDataRepository;

    public DatabaseTableReferenceLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, JoinTableReferenceSqlFactory.INSTANCE);
    }

    public DatabaseTableReferenceLoader(IDatabaseConnection databaseConnection, TableReferenceSqlFactory tableReferenceSqlFactory) throws DataSetException {
        this.databaseConnection = Objects.requireNonNull(databaseConnection);

        this.tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        this.tableReferenceSqlFactory = Objects.requireNonNull(tableReferenceSqlFactory);
    }


    @Override
    public ITable loadReferencedTable(ITable sourceTable, TableReference tableReference, TableReference.Direction direction) throws DataSetException {
        TableReference.Edge sourceEdge = direction.getSourceEdge(tableReference);
        TableReference.Edge targetEdge = direction.getTargetEdge(tableReference);

        try {
            SqlStatement sqlStatement = tableReferenceSqlFactory.create(sourceTable, sourceEdge, targetEdge);
            Connection connection = databaseConnection.getConnection();
            return sqlStatement.executeQuery(connection, ps -> {
                return loadTableFromResultSet(ps.executeQuery());
            });
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    private ITable loadTableFromResultSet(ResultSet resultSet) throws DataSetException, SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String tableName = metaData.getTableName(1);
        ITableMetaData targetTableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
        ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
        return new CachedResultSetTable(forwardOnlyResultSetTable);
    }
}
