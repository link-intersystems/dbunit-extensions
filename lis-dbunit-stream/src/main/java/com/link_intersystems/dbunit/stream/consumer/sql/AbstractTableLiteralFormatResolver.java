package com.link_intersystems.dbunit.stream.consumer.sql;

import com.link_intersystems.sql.format.DefaultTableLiteralFormat;
import com.link_intersystems.sql.format.LiteralFormat;
import com.link_intersystems.sql.format.QuotedStringLiteralFormat;
import com.link_intersystems.sql.statement.TableLiteralFormat;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractTableLiteralFormatResolver implements TableLiteralFormatResolver {

    private Map<ITableMetaData, TableLiteralFormat> cache = new LinkedHashMap<ITableMetaData, TableLiteralFormat>() {

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 32;
        }
    };

    @Override
    public TableLiteralFormat getTableLiteralFormat(ITableMetaData tableMetaData) throws DataSetException {
        TableLiteralFormat tableLiteralFormat = cache.get(tableMetaData);

        if (tableLiteralFormat == null) {
            tableLiteralFormat = createTableLiteralFormat(tableMetaData);
            cache.put(tableMetaData, tableLiteralFormat);
        }

        return tableLiteralFormat;
    }

    protected TableLiteralFormat createTableLiteralFormat(ITableMetaData tableMetaData) throws DataSetException {
        DefaultTableLiteralFormat tableLiteralFormat = new DefaultTableLiteralFormat();

        tableLiteralFormat.setDefaultLiteralFormat(new QuotedStringLiteralFormat());

        for (Column column : tableMetaData.getColumns()) {
            String columnName = column.getColumnName();
            LiteralFormat literalFormat = getLiteralFormat(tableMetaData, column);
            tableLiteralFormat.addLiteralFormat(columnName, literalFormat);
        }

        return tableLiteralFormat;
    }

    protected abstract LiteralFormat getLiteralFormat(ITableMetaData tableMetaData, Column column);
}
