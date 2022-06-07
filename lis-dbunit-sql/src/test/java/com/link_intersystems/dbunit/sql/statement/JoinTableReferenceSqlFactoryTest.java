package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class JoinTableReferenceSqlFactoryTest extends AbstractTableReferenceSqlFactoryTest {

    @Override
    protected TableReferenceSqlFactory createTableReferenceSqlFactory() {
        return JoinTableReferenceSqlFactory.INSTANCE;
    }

    @Override
    protected void verify(SqlStatement sqlStatement) {
        CharSequence sql = sqlStatement.getSql();
        assertEquals("select distinct target_table.* from target_table join source_table on source_table.fk1 = target_table.id1 and source_table.fk2 = target_table.id2 where (source_table.fk1, source_table.fk2) in ((?, ?), (?, ?))", sql);

        List<Object> arguments = sqlStatement.getArguments();
        assertEquals(Arrays.asList(1, "hello", 2, "world"), arguments);
    }
}