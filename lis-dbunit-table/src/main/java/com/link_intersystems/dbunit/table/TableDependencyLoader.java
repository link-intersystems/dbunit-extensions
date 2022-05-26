package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.DependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.JoinDependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.jdbc.ConnectionMetaData;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableDependencyLoader {


    public static enum DependencyDirection {
        OUTGOING {
            @Override
            public List<TableReference> getDependencies(ConnectionMetaData connectionMetaData, String tableName) throws SQLException {
                return connectionMetaData.getImportedKeys(tableName).stream().map(TableReference::new).collect(Collectors.toList());
            }

            @Override
            public TableReference.Edge getSourceEdge(TableReference dependency) {
                return dependency.getSourceEdge();
            }

            @Override
            public TableReference.Edge getTargetEdge(TableReference dependency) {
                return dependency.getTargetEdge();
            }
        }, INCOMING {
            @Override
            public List<TableReference> getDependencies(ConnectionMetaData connectionMetaData, String tableName) throws SQLException {
                return connectionMetaData.getExportedKeys(tableName).stream().map(TableReference::new).collect(Collectors.toList());
            }

            @Override
            public TableReference.Edge getSourceEdge(TableReference dependency) {
                return dependency.getTargetEdge();
            }

            @Override
            public TableReference.Edge getTargetEdge(TableReference dependency) {
                return dependency.getSourceEdge();
            }
        };

        public abstract List<TableReference> getDependencies(ConnectionMetaData connectionMetaData, String tableName) throws DataSetException, SQLException;

        public abstract TableReference.Edge getSourceEdge(TableReference dependency);

        public abstract TableReference.Edge getTargetEdge(TableReference dependency);
    }

    private final IDatabaseConnection databaseConnection;

    private DependencyStatementFactory dependencyStatementFactory;
    private final TableMetaDataRepository tableMetaDataRepository;
    private final ConnectionMetaData connectionMetaData;

    public TableDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, JoinDependencyStatementFactory.INSTANCE);
    }

    public TableDependencyLoader(IDatabaseConnection databaseConnection, DependencyStatementFactory dependencyStatementFactory) throws DataSetException {
        this.databaseConnection = databaseConnection;

        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        try {
            connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection());
            this.dependencyStatementFactory = Objects.requireNonNull(dependencyStatementFactory);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }


    public void loadTables(ITable sourceTable, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();

        try {
            List<TableReference> dependencies = direction.getDependencies(connectionMetaData, tableMetaData.getTableName());


            for (TableReference dependency : dependencies) {
                if (loadContext.follow(dependency)) {
                    loadDependency(sourceTable, dependency, direction, loadContext);
                }
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private void loadDependency(ITable sourceTable, TableReference dependency, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        TableReference.Edge sourceEdge = direction.getSourceEdge(dependency);
        TableReference.Edge targetEdge = direction.getTargetEdge(dependency);

        try {
            SqlStatement sqlStatement = dependencyStatementFactory.create(sourceTable, sourceEdge, targetEdge);
            Connection connection = databaseConnection.getConnection();
            sqlStatement.executeQuery(connection, ps -> {
                loadTableFromResultSet(ps.executeQuery(), loadContext);
            });
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    private ITable loadTableFromResultSet(ResultSet resultSet, TableContext loadContext) throws DataSetException, SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String tableName = metaData.getTableName(1);
        ITableMetaData targetTableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
        ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
        ITable targetTable = new CachedResultSetTable(forwardOnlyResultSetTable);
        loadContext.add(targetTable);
        return targetTable;
    }
}
