package com.link_intersystems.dbunit.sql.statement;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ExistsSubqueryTableReferenceSqlFactoryTest extends AbstractTableReferenceSqlFactoryTest {

    @Override
    protected TableReferenceSqlFactory createTableReferenceSqlFactory() {
        return ExistsSubqueryTableReferenceSqlFactory.INSTANCE;
    }

    @Override
    protected void verify(SqlStatement sqlStatement) {
        CharSequence sql = sqlStatement.getSql();
        assertEquals("select * from target_table t where exists(select * from source_table s where s.fk1 = t.id1 and s.fk2 = t.id2 and ((s.fk1 = ? and s.fk2 = ?) or (s.fk1 = ? and s.fk2 = ?)))", sql);

        List<Object> arguments = sqlStatement.getArguments();
        assertEquals(Arrays.asList(1, "hello", 2, "world"), arguments);
    }
}