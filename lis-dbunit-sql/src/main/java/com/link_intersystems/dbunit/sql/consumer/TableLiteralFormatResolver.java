package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.sql.statement.TableLiteralFormat;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableLiteralFormatResolver {
    TableLiteralFormat getTableLiteralFormat(ITableMetaData tableMetaData) throws DataSetException;
}
