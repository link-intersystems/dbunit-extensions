package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
@SakilaSlimExtension
class RowTest {

    private SakilaDBFixture sakilaDBFixture;
    private TableUtil actorTableUtil;
    private ITable actorTable;
    private Row row;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        sakilaDBFixture = new SakilaDBFixture(connection);
        actorTable = sakilaDBFixture.getTable("actor");
        actorTableUtil = new TableUtil(actorTable);

        row = actorTableUtil.getRowById(2);
    }

    @Test
    void nullCells() {
        assertThrows(NullPointerException.class, () -> new Row(null));
    }

    @Test
    void toMap() {
        Map<String, Object> rowAsMap = row.toMap();

        assertEquals(4, rowAsMap.size());

        assertEquals(2, rowAsMap.get("actor_id"));
        assertEquals("NICK", rowAsMap.get("first_name"));
        assertEquals("WAHLBERG", rowAsMap.get("last_name"));
    }

    @Test
    void get() {
        assertEquals(2, row.get(0));
        assertEquals("NICK", row.get(1));
        assertEquals("WAHLBERG", row.get(2));
    }

    @Test
    void size() {
        assertEquals(4, row.size());
    }

    @Test
    void getColumns() {
        Column[] columns = row.getColumns();

        assertEquals(4, columns.length);

        assertEquals("actor_id", columns[0].getColumnName());
        assertEquals("first_name", columns[1].getColumnName());
        assertEquals("last_name", columns[2].getColumnName());
        assertEquals("last_update", columns[3].getColumnName());
    }

    @Test
    void ignoreCase() {
        assertEquals("NICK", row.getValue("first_name"));

        assertThrows(IllegalArgumentException.class, () -> row.getValue("FIRST_NAME"));

        assertEquals("NICK", row.ignoreCase().getValue("FIRST_NAME"));
    }
}