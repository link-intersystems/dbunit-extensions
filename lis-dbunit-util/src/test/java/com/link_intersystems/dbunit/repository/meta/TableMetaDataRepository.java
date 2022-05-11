package com.link_intersystems.dbunit.repository.meta;

import com.link_intersystems.dbunit.repository.jdbc.JdbcColumnMetaData;
import com.link_intersystems.dbunit.repository.jdbc.JdbcMetaDataRepository;
import com.link_intersystems.dbunit.repository.jdbc.JdbcTableMetaData;
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

    private JdbcMetaDataRepository jdbcMetaDataRepository;
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
        this.databaseConnection = databaseConnection;
        try {
            jdbcMetaDataRepository = new JdbcMetaDataRepository(databaseConnection.getConnection(), TableType.toNames(tableTypes));
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        try {
            JdbcTableMetaData jdbcTableMetaData = jdbcMetaDataRepository.getTableMetaData(tableName);
            IDataTypeFactory dataTypeFactory = abstractTableMetaData.getDataTypeFactory(databaseConnection);

            List<JdbcColumnMetaData> columnMetaDataList = jdbcMetaDataRepository.getColumnMetaDataList(jdbcTableMetaData);
            List<Column> columns = new ArrayList<>();

            for (JdbcColumnMetaData jdbcColumnMetaData : columnMetaDataList) {
                Column columnFromDbMetaData = createColumnFromDbMetaData(jdbcColumnMetaData, dataTypeFactory);
                columns.add(columnFromDbMetaData);
            }

            return new DefaultTableMetaData(tableName, columns.stream().toArray(Column[]::new));
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private Column createColumnFromDbMetaData(JdbcColumnMetaData jdbcColumnMetaData, IDataTypeFactory dataTypeFactory) throws SQLException, DataTypeException {

        if (logger.isTraceEnabled()) {
            logger.trace("createColumnFromMetaData(jdbcColumnMetaData={}, dataTypeFactory={}) - start", new Object[]{jdbcColumnMetaData, dataTypeFactory});
        }

        String catalogName = jdbcColumnMetaData.getCatalogName();
        String schemaName = jdbcColumnMetaData.getSchemaName();
        String tableName = jdbcColumnMetaData.getTypeName();
        String columnName = jdbcColumnMetaData.getColumnName();

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
                Column column = createColumn(jdbcColumnMetaData, dataTypeFactory);
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

    private Column createColumn(JdbcColumnMetaData jdbcColumnMetaData, IDataTypeFactory dataTypeFactory) throws DataTypeException {
        String tableName = jdbcColumnMetaData.getTableName();
        String columnName = jdbcColumnMetaData.getColumnName();
        int sqlType = jdbcColumnMetaData.getDataType();
        if (sqlType == 2001) {
            sqlType = jdbcColumnMetaData.getSourceDataType();
        }

        String sqlTypeName = jdbcColumnMetaData.getTypeName();
        int nullable = jdbcColumnMetaData.getNullable();
        String remarks = jdbcColumnMetaData.getRemarks();
        String columnDefaultValue = jdbcColumnMetaData.getColumnDefaultValue();
        String isAutoIncrement = Column.AutoIncrement.NO.getKey();

        try {
            isAutoIncrement = jdbcColumnMetaData.getIsAutoincrement();
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
