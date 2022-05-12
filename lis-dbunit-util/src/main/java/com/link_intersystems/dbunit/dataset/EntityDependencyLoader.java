package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.meta.Dependency;
import com.link_intersystems.dbunit.dataset.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.dataset.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.dataset.table.DistinctCompositeTable;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EntityDependencyLoader {

    private final TableDependencyRepository tableDependencyRepository;
    private IDatabaseConnection databaseConnection;
    private final TableMetaDataRepository tableMetaDataRepository;
    private final DependentEntityStatement dependentEntityStatement;


    public EntityDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);
        this.databaseConnection = databaseConnection;
        try {
            dependentEntityStatement = new DependentEntityStatement(databaseConnection.getConnection());
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public List<ITable> getOutgoingTables(ITable sourceTable) throws DataSetException {
        Map<String, ITable> tableContext = new LinkedHashMap<>();
        getOutgoingTablesImpl(sourceTable, tableContext);
        return new ArrayList<>(tableContext.values());
    }


    private void getOutgoingTablesImpl(ITable sourceTable, Map<String, ITable> tableContext) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        List<Dependency> outgoingDependencies = tableDependencyRepository.getOutgoingDependencies(tableMetaData.getTableName());

        for (Dependency outgoingDependency : outgoingDependencies) {
            Dependency.Edge targetEdge = outgoingDependency.getTargetEdge();

            try (PreparedStatement ps = dependentEntityStatement.create(sourceTable, outgoingDependency)) {
                if (ps.execute()) {
                    ResultSet resultSet = ps.getResultSet();
                    ResultSetTableMetaData2 targetTableMetaData = new ResultSetTableMetaData2(targetEdge.getTableMetaData().getTableName(), resultSet, databaseConnection);
                    ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
                    ITable targetTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                    String tableName = targetTableMetaData.getTableName();
                    ITable existingTable = tableContext.get(tableName);
                    if (existingTable != null) {
                        targetTable = new DistinctCompositeTable(targetTableMetaData, targetTable, existingTable);
                    }
                    tableContext.put(tableName, targetTable);
                    getOutgoingTablesImpl(targetTable, tableContext);
                }
            } catch (SQLException e) {
                throw new DataSetException(e);
            }

        }
    }


}
