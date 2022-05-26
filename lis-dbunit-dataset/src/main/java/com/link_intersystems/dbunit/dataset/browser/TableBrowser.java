package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dataset.MergedDataSet;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.dbunit.table.TableContext;
import com.link_intersystems.jdbc.ConnectionMetaData;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowser {

    public static IDataSet browse(IDatabaseConnection databaseConnection, BrowseTable tableBrowseRef) throws DataSetException {
        TableBrowser tableBrowser = new TableBrowser(databaseConnection);
        tableBrowser.browse(tableBrowseRef);
        return tableBrowser.getDataSet();
    }

    private TableContext tableContext = new TableContext();

    private final IDatabaseConnection databaseConnection;
    private final TableMetaDataRepository tableMetaDataRepository;
    private final ConnectionMetaData connectionMetaData;

    private DefaultTableBrowseSqlFactory tableBrowseSqlFactory;

    public TableBrowser(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        try {
            connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection());
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    protected TableBrowseSqlFactory getTableBrowseSqlFactory() {
        if (tableBrowseSqlFactory == null) {
            tableBrowseSqlFactory = new DefaultTableBrowseSqlFactory(connectionMetaData);
        }
        return tableBrowseSqlFactory;
    }


    public void browse(BrowseTable tableBrowseRef) throws DataSetException {
        SqlStatement sqlStatement = createSqlStatement(tableBrowseRef);
        browse(tableBrowseRef, sqlStatement);
    }

    protected SqlStatement createSqlStatement(BrowseTable tableBrowseRef) {
        TableBrowseSqlFactory tableBrowseSqlFactory = getTableBrowseSqlFactory();
        return tableBrowseSqlFactory.createSqlStatement(tableBrowseRef);
    }

    private void browse(BrowseTable targetTableRef, SqlStatement sqlStatement) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            ITable targetTable = sqlStatement.processResultSet(connection, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                String tableName = metaData.getTableName(1);

                ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, rs);
                return new CachedResultSetTable(forwardOnlyResultSetTable);
            });

            tableContext.add(targetTable);

            List<BrowseTableReference> browseReferences = targetTableRef.getReferences();

            for (BrowseTableReference browseReference : browseReferences) {
                browseRef(targetTable, browseReference);
            }

        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private void browseRef(ITable sourceTable, BrowseTableReference targetBrowseReference) throws SQLException {
        BrowseTable targetTableRef = targetBrowseReference.getTargetTableRef();

        try {
            TableBrowseSqlFactory tableBrowseSqlFactory = getTableBrowseSqlFactory();

            SqlStatement sqlStatement = tableBrowseSqlFactory.createSqlStatement(targetBrowseReference, sourceTable);

            browse(targetTableRef, sqlStatement);
        } catch (Exception e) {
            String msg = MessageFormat.format("Can not browse from source table ''{0}'' to target table ''{1}''", sourceTable.getTableMetaData().getTableName(), targetTableRef.getTableName());
            throw new TableBrowseException(msg, e);
        }
    }

    public IDataSet getDataSet() {
        return new MergedDataSet(tableContext);
    }

}
