package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.sql.statement.TableLiteralFormat;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableLiteralFormatResolver {
    /**
     * @param tableMetaData
     * @return
     * @throws DataSetException
     * @see <a href="https://github.com/link-intersystems/lis-commons/blob/master/lis-commons-sql/src/main/java/com/link_intersystems/sql/statement/TableLiteralFormat.java">TableLiteralFormat</a>
     */
    TableLiteralFormat getTableLiteralFormat(ITableMetaData tableMetaData) throws DataSetException;
}
