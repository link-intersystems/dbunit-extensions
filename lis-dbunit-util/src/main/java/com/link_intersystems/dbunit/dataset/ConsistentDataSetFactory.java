package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.dataset.meta.TableMetaDataRepository;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDataSetFactory {

    private final IDatabaseConnection databaseConnection;
    private final TableDependencyRepository tableDependencyRepository;
    private final TableMetaDataRepository tableMetaDataRepository;


    public ConsistentDataSetFactory(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);

    }

    public IDataSet createDataSet(String sqlQuery, Object... args) throws DataSetException {


        try {
            Connection connection = databaseConnection.getConnection();
            List<ITable> tables = createDataSet(connection, sqlQuery, args);
            return new DefaultDataSet(tables.toArray(new ITable[0]));
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private List<ITable> createDataSet(Connection connection, String sqlQuery, Object[] args) throws SQLException, DataSetException {
        List<ITable> dataSetTables = new ArrayList<>();

        EntityDependencyLoader entityDependencyLoader = new EntityDependencyLoader(databaseConnection);

        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[0]);
            }


            if (ps.execute()) {
                ResultSet resultSet = ps.getResultSet();
                ResultSetMetaData metaData = resultSet.getMetaData();
                String tableName = metaData.getTableName(1);

                ResultSetTableMetaData2 tableMetaData = new ResultSetTableMetaData2(tableName, resultSet, databaseConnection);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, resultSet);
                CachedResultSetTable mainTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                dataSetTables.add(mainTable);
                List<ITable> outgoingTables = entityDependencyLoader.getOutgoingTables(mainTable);
                dataSetTables.addAll(outgoingTables);
            }
        }

        return dataSetTables;
    }


}

