package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.LisDatabaseConfig;
import com.link_intersystems.dbunit.meta.Dependency;
import com.link_intersystems.dbunit.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.statement.DependencyStatementFactory;
import com.link_intersystems.dbunit.statement.ExistsSubqueryDependencyStatementFactory;
import com.link_intersystems.dbunit.statement.SqlStatement;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableDependencyLoader {

    private final TableDependencyRepository tableDependencyRepository;
    private IDatabaseConnection databaseConnection;
    private final TableMetaDataRepository tableMetaDataRepository;

    private static final ThreadLocal<Map<String, ITable>> tableContextHolder = new ThreadLocal<>();

    public TableDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);
        this.databaseConnection = databaseConnection;
    }

    public List<ITable> getOutgoingTables(ITable sourceTable) throws DataSetException {
        try {
            tableContextHolder.set(new LinkedHashMap<>());
            loadOutgoingTables(sourceTable);
            Map<String, ITable> tableContext = tableContextHolder.get();
            return new ArrayList<>(tableContext.values());
        } finally {
            tableContextHolder.remove();
        }
    }


    private void loadOutgoingTables(ITable sourceTable) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        List<Dependency> outgoingDependencies = tableDependencyRepository.getOutgoingDependencies(tableMetaData.getTableName());

        for (Dependency outgoingDependency : outgoingDependencies) {
            loadOutgoingDependency(sourceTable, outgoingDependency);
        }
    }

    private void loadOutgoingDependency(ITable sourceTable, Dependency outgoingDependency) throws DataSetException {
        Dependency.Edge targetEdge = outgoingDependency.getTargetEdge();

        DatabaseConfig config = databaseConnection.getConfig();
        DependencyStatementFactory dependencyStatementFactory = getDependencyStatementFactory();
        SqlStatement sqlStatement = dependencyStatementFactory.create(config, sourceTable, outgoingDependency);

        try {
            Connection connection = databaseConnection.getConnection();
            sqlStatement.executeQuery(connection, ps -> {
                ResultSet resultSet = ps.executeQuery();
                String targetTableName = targetEdge.getTableMetaData().getTableName();
                ITableMetaData targetTableMetaData = tableMetaDataRepository.getTableMetaData(targetTableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
                ITable targetTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                String tableName = targetTableMetaData.getTableName();

                Map<String, ITable> tableContext = tableContextHolder.get();

                ITable existingTable = tableContext.get(tableName);
                if (existingTable != null) {
                    targetTable = new DistinctCompositeTable(targetTableMetaData, targetTable, existingTable);
                }
                tableContext.put(tableName, targetTable);
                loadOutgoingTables(targetTable);
            });
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    private DependencyStatementFactory getDependencyStatementFactory() {
        DatabaseConfig config = databaseConnection.getConfig();

        DependencyStatementFactory dependencyStatementFactory = (DependencyStatementFactory) config.getProperty(LisDatabaseConfig.PROPERTY_DEPENDENCY_STATEMENT_FACTORY);

        if (dependencyStatementFactory == null) {
            dependencyStatementFactory = ExistsSubqueryDependencyStatementFactory.INSTANCE;
        }

        return dependencyStatementFactory;
    }


}
