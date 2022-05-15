package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.table.ListSnapshot;
import com.link_intersystems.dbunit.table.TableContext;
import com.link_intersystems.dbunit.table.TableDependencyLoader;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDataSetLoader {

    private final IDatabaseConnection databaseConnection;
    private final TableDependencyRepository tableDependencyRepository;
    private final TableMetaDataRepository tableMetaDataRepository;


    public ConsistentDataSetLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);

    }

    public IDataSet load(String sqlQuery, Object... args) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            List<ITable> tables = load(connection, sqlQuery, args);
            return new DefaultDataSet(tables.toArray(new ITable[0]));
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private List<ITable> load(Connection connection, String sqlQuery, Object[] args) throws SQLException, DataSetException {
        List<ITable> dataSetTables = new ArrayList<>();

        TableDependencyLoader entityDependencyLoader = new TableDependencyLoader(databaseConnection);

        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[0]);
            }

            if (ps.execute()) {
                ResultSet resultSet = ps.getResultSet();
                ResultSetMetaData metaData = resultSet.getMetaData();
                String tableName = metaData.getTableName(1);

                ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, resultSet);
                CachedResultSetTable mainTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                List<ITable> outgoingTables = loadOutgoingTables(entityDependencyLoader, mainTable);
                dataSetTables.addAll(outgoingTables);
            }
        }

        return dataSetTables;
    }

    private List<ITable> loadOutgoingTables(TableDependencyLoader entityDependencyLoader, ITable table) throws DataSetException {
        TableContext tableContext = new TableContext();
        tableContext.add(table);
        loadOutgoingTables(entityDependencyLoader, table, tableContext);
        return tableContext;
    }

    private void loadOutgoingTables(TableDependencyLoader entityDependencyLoader, ITable table, TableContext tableContext) throws DataSetException {
        ListSnapshot<ITable> beforeLoad = tableContext.getSnapshot();
        entityDependencyLoader.loadOutgoingTables(table, tableContext);
        List<ITable> loadedTables = beforeLoad.diff(tableContext);

        for (ITable outgoingTable : loadedTables) {
            loadOutgoingTables(entityDependencyLoader, outgoingTable, tableContext);
        }
    }


}

