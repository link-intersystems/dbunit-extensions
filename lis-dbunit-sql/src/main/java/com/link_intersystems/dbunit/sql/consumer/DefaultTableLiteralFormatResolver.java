package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.sql.format.*;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultTableLiteralFormatResolver extends AbstractTableLiteralFormatResolver {

    private Map<Integer, LiteralFormat> sqlTypeLiteralFormats;

    private LiteralFormat defaultLiteralFormat = new SimpleLiteralFormat();

    public void setDefaultLiteralFormat(LiteralFormat defaultLiteralFormat) {
        this.defaultLiteralFormat = requireNonNull(defaultLiteralFormat);
    }

    private Map<Integer, LiteralFormat> getSqlTypeLiteralFormats() {
        if (sqlTypeLiteralFormats == null) {
            sqlTypeLiteralFormats = new HashMap<>();
            initSqlTypeLiteralFormats(sqlTypeLiteralFormats);
        }

        return sqlTypeLiteralFormats;
    }

    protected void initSqlTypeLiteralFormats(Map<Integer, LiteralFormat> sqlTypeLiteralFormats) {
        QuotedStringLiteralFormat quotedStringLiteralFormat = new QuotedStringLiteralFormat();
        sqlTypeLiteralFormats.put(Types.CHAR, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.NCHAR, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.VARCHAR, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.LONGVARCHAR, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.LONGNVARCHAR, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.CLOB, quotedStringLiteralFormat);
        sqlTypeLiteralFormats.put(Types.NCLOB, quotedStringLiteralFormat);

        DecimalLiteralFormat decimalLiteralFormat = new DecimalLiteralFormat();
        sqlTypeLiteralFormats.put(Types.DECIMAL, decimalLiteralFormat);
        sqlTypeLiteralFormats.put(Types.NUMERIC, decimalLiteralFormat);

        SimpleLiteralFormat simpleLiteralFormat = new SimpleLiteralFormat();
        sqlTypeLiteralFormats.put(Types.BOOLEAN, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.BIGINT, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.DOUBLE, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.FLOAT, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.TINYINT, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.SMALLINT, simpleLiteralFormat);
        sqlTypeLiteralFormats.put(Types.INTEGER, simpleLiteralFormat);

        TimestampLiteralFormat timestampLiteralFormat = getTimestampLiteralFormat();
        sqlTypeLiteralFormats.put(Types.TIMESTAMP, timestampLiteralFormat);

        DateLiteralFormat literalFormat = getDateLiteralFormat();
        sqlTypeLiteralFormats.put(Types.DATE, literalFormat);
    }

    protected DateLiteralFormat getDateLiteralFormat() {
        return new DateLiteralFormat();
    }

    protected TimestampLiteralFormat getTimestampLiteralFormat() {
        return new TimestampLiteralFormat();
    }

    @Override
    protected LiteralFormat getLiteralFormat(ITableMetaData tableMetaData, Column column) {

        DataType dataType = column.getDataType();
        int sqlType = dataType.getSqlType();

        LiteralFormat literalFormat = getSqlTypeLiteralFormats().get(sqlType);
        if (literalFormat != null) {
            return literalFormat;
        }

        literalFormat = getDatabaseSpecificTypeLiteralFormat(tableMetaData, column);
        if (literalFormat != null) {
            return literalFormat;
        }

        return defaultLiteralFormat;

    }

    protected LiteralFormat getDatabaseSpecificTypeLiteralFormat(ITableMetaData tableMetaData, Column column) {
        return new QuotedStringLiteralFormat();
    }
}
