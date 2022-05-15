package com.link_intersystems.dbunit.meta;

import com.link_intersystems.jdbc.*;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableMetaDataRepository {

    private static final Logger logger = LoggerFactory.getLogger(TableMetaDataRepository.class);

    private ConnectionMetaData connectionMetaData;
    private AbstractTableMetaData abstractTableMetaData = new AbstractTableMetaData() {
        @Override
        public String getTableName() {
            return null;
        }

        @Override
        public Column[] getColumns() throws DataSetException {
            return new Column[0];
        }

        @Override
        public Column[] getPrimaryKeys() throws DataSetException {
            return new Column[0];
        }
    };

    public enum TableType {
        TABLE, VIEW;

        static String[] toNames(TableType... tableTypes) {
            return Arrays.stream(tableTypes).map(Enum::name).toArray(String[]::new);
        }
    }

    private IDatabaseConnection databaseConnection;

    public TableMetaDataRepository(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, TableType.TABLE);
    }

    public TableMetaDataRepository(IDatabaseConnection databaseConnection, TableType... tableTypes) throws DataSetException {
        this(databaseConnection, null, tableTypes);
    }

    public TableMetaDataRepository(IDatabaseConnection databaseConnection, JdbcContext jdbcContext, TableType... tableTypes) throws DataSetException {
        this.databaseConnection = databaseConnection;
        try {
            connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection(), jdbcContext, TableType.toNames(tableTypes));
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        try {
            TableMetaData tableMetaData = connectionMetaData.getTableMetaData(tableName);
            IDataTypeFactory dataTypeFactory = abstractTableMetaData.getDataTypeFactory(databaseConnection);

            List<ColumnMetaData> columnMetaDataList = connectionMetaData.getColumnMetaDataList(tableMetaData);
            List<Column> columns = new ArrayList<>();

            for (ColumnMetaData jdbcColumnMetaData : columnMetaDataList) {
                Column columnFromDbMetaData = createColumnFromDbMetaData(jdbcColumnMetaData, dataTypeFactory);
                columns.add(columnFromDbMetaData);
            }

            PrimaryKey primaryKey = connectionMetaData.getPrimaryKey(tableName);
            String[] primaryKeyNames = primaryKey.stream().map(ColumnMetaData::getColumnName).toArray(String[]::new);

            return new DefaultTableMetaData(tableName, columns.stream().toArray(Column[]::new), primaryKeyNames);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private Column createColumnFromDbMetaData(ColumnMetaData columnMetaData, IDataTypeFactory dataTypeFactory) throws SQLException, DataTypeException {

        if (logger.isTraceEnabled()) {
            logger.trace("createColumnFromMetaData(columnMetaData={}, dataTypeFactory={}) - start", new Object[]{columnMetaData, dataTypeFactory});
        }

        String catalogName = columnMetaData.getCatalogName();
        String schemaName = columnMetaData.getSchemaName();
        String tableName = columnMetaData.getTypeName();
        String columnName = columnMetaData.getColumnName();

        catalogName = this.trim(catalogName);
        schemaName = this.trim(schemaName);
        tableName = this.trim(tableName);
        columnName = this.trim(columnName);
        if (catalogName != null && catalogName.equals("")) {
            catalogName = null;
        }

        if (schemaName != null && schemaName.equals("")) {
            logger.debug("The 'schemaName' from the ResultSetMetaData is empty-string and not applicable hence. Will not try to lookup column properties via DatabaseMetaData.getColumns.");
            return null;
        } else if (tableName != null && tableName.equals("")) {
            logger.debug("The 'tableName' from the ResultSetMetaData is empty-string and not applicable hence. Will not try to lookup column properties via DatabaseMetaData.getColumns.");
            return null;
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("All attributes from the ResultSetMetaData are valid, trying to lookup values in DatabaseMetaData. catalog={}, schema={}, table={}, column={}", new Object[]{catalogName, schemaName, tableName, columnName});
            }

            Column var12;
            try {
                Column column = createColumn(columnMetaData, dataTypeFactory);
                var12 = column;
                return var12;
            } catch (IllegalStateException var16) {
                logger.warn("Cannot find column from ResultSetMetaData info via DatabaseMetaData. Returning null. Even if this is expected to never happen it probably happened due to a JDBC driver bug. To get around this you may want to configure a user defined " + IMetadataHandler.class, var16);
                var12 = null;
            }

            return var12;
        }
    }

    private String trim(String value) {
        return value == null ? null : value.trim();
    }

    private Column createColumn(ColumnMetaData columnMetaData, IDataTypeFactory dataTypeFactory) throws DataTypeException {
        String tableName = columnMetaData.getTableName();
        String columnName = columnMetaData.getColumnName();
        int sqlType = columnMetaData.getDataType();
        if (sqlType == 2001) {
            sqlType = columnMetaData.getSourceDataType();
        }

        String sqlTypeName = columnMetaData.getTypeName();
        int nullable = columnMetaData.getNullable();
        String remarks = columnMetaData.getRemarks();
        String columnDefaultValue = columnMetaData.getColumnDefaultValue();
        String isAutoIncrement = Column.AutoIncrement.NO.getKey();

        try {
            isAutoIncrement = columnMetaData.getIsAutoincrement();
        } catch (Exception var13) {
            String msg = "Could not retrieve the 'isAutoIncrement' property because not yet running on Java 1.5 - defaulting to NO. Table={}, Column={}";
            logger.debug("Could not retrieve the 'isAutoIncrement' property because not yet running on Java 1.5 - defaulting to NO. Table={}, Column={}", new Object[]{tableName, columnName, var13});
        }

        DataType dataType = dataTypeFactory.createDataType(sqlType, sqlTypeName, tableName, columnName);
        if (dataType != DataType.UNKNOWN) {
            Column column = new Column(columnName, dataType, sqlTypeName, Column.nullableValue(nullable), columnDefaultValue, remarks, Column.AutoIncrement.autoIncrementValue(isAutoIncrement));
            return column;
        } else {
            return null;
        }
    }

    private static boolean isCaseSensitiveConfigured(IDatabaseConnection connection) {
        return connection.getConfig().getFeature("http://www.dbunit.org/features/caseSensitiveTableNames");
    }
}
