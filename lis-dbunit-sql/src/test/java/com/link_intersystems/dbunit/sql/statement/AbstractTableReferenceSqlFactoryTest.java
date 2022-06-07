package com.link_intersystems.dbunit.sql.statement;

import com.link_intersystems.jdbc.TableReference;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractTableReferenceSqlFactoryTest {
    protected DefaultTable sourceTable;
    protected TableReference.Edge sourceEdge;
    protected TableReference.Edge targetEdge;

    @BeforeEach
    void setUp() throws DataSetException {
        Column fk1Column = new Column("fK1", DataType.BIGINT);
        Column fk2Column = new Column("fk2", DataType.VARCHAR);
        Column[] filmActorColumns = new Column[]{fk1Column, fk2Column};
        sourceTable = new DefaultTable("source_table", filmActorColumns);
        sourceTable.addRow(new Object[]{1, "hello"});
        sourceTable.addRow(new Object[]{2, "world"});
        sourceEdge = new TableReference.Edge("source_table", Arrays.asList("fk1", "fk2"));
        targetEdge = new TableReference.Edge("target_table", Arrays.asList("id1", "id2"));
    }

    @Test
    void createSql() throws Exception {
        TableReferenceSqlFactory instance = createTableReferenceSqlFactory();

        SqlStatement sqlStatement = instance.create(sourceTable, sourceEdge, targetEdge);

        verify(sqlStatement);
    }

    protected abstract void verify(SqlStatement sqlStatement);


    protected abstract TableReferenceSqlFactory createTableReferenceSqlFactory();
}
