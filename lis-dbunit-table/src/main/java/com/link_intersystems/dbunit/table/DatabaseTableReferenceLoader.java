package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.JoinTableReferenceSqlFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.dbunit.sql.statement.TableReferenceSqlFactory;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceList;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseTableReferenceLoader implements TableReferenceLoader {

    private final IDatabaseConnection databaseConnection;

    private TableReferenceSqlFactory tableReferenceSqlFactory;
    private final TableMetaDataRepository tableMetaDataRepository;
    private final ConnectionMetaData connectionMetaData;

    public DatabaseTableReferenceLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, JoinTableReferenceSqlFactory.INSTANCE);
    }

    public DatabaseTableReferenceLoader(IDatabaseConnection databaseConnection, TableReferenceSqlFactory tableReferenceSqlFactory) throws DataSetException {
        this.databaseConnection = Objects.requireNonNull(databaseConnection);

        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        try {
            connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection());
            this.tableReferenceSqlFactory = Objects.requireNonNull(tableReferenceSqlFactory);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public TableList loadOutgoingReferences(ITable sourceTable) throws DataSetException {
        return loadOutgoingReferences(sourceTable, tr -> true);
    }

    @Override
    public TableList loadOutgoingReferences(ITable sourceTable, Predicate<TableReference> referenceFilter) throws DataSetException {
        List<ITable> tables = new ArrayList<>();

        ITableMetaData tableMetaData = sourceTable.getTableMetaData();

        try {
            String tableName = tableMetaData.getTableName();
            TableReferenceList outgoingReferences = connectionMetaData.getOutgoingReferences(tableName);

            for (TableReference outgoingReference : outgoingReferences) {
                if (referenceFilter.test(outgoingReference)) {
                    ITable referencedTable = loadReference(sourceTable, outgoingReference, TableReference.Direction.NATURAL);
                    tables.add(referencedTable);
                }
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }

        return new TableList(tables);
    }

    private ITable loadReference(ITable sourceTable, TableReference tableReference, TableReference.Direction direction) throws DataSetException {
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
