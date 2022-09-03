package com.link_intersystems.dbunit.meta;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.jdbc.test.db.h2.H2Extension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(H2Extension.class)
@SakilaSlimExtension
class TableMetaDataBuilderTest {

    private DatabaseDataSet databaseDataSet;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException, SQLException {
        databaseDataSet = new DatabaseDataSet(new DatabaseConnection(connection), false);
    }

    @Test
    void build() throws DataSetException {
        ITableMetaData sakilaActor = databaseDataSet.getTableMetaData("actor");

        TableMetaDataBuilder actor = new TableMetaDataBuilder("actor");
        ColumnListBuilder columnListBuilder = new ColumnListBuilder(sakilaActor.getColumns());
        actor.setColumns(columnListBuilder.build().toArray());
        actor.setPkColumns(new ColumnList(sakilaActor.getPrimaryKeys()).toColumnNames());

        ITableMetaData tableMetaData = actor.build();

        DBUnitAssertions.STRICT.assertMetaDataEquals(sakilaActor, tableMetaData);
        assertNotSame(sakilaActor.getColumns()[0], tableMetaData.getColumns()[0]);
        assertNotSame(sakilaActor.getPrimaryKeys()[0], tableMetaData.getPrimaryKeys()[0]);
    }

    @Test
    void buildFromTemplate() throws DataSetException {
        ITableMetaData sakilaActor = databaseDataSet.getTableMetaData("actor");

        TableMetaDataBuilder actor = new TableMetaDataBuilder(sakilaActor);
        ITableMetaData tableMetaData = actor.build();

        DBUnitAssertions.STRICT.assertMetaDataEquals(sakilaActor, tableMetaData);
        assertNotSame(sakilaActor.getColumns()[0], tableMetaData.getColumns()[0]);
        assertNotSame(sakilaActor.getPrimaryKeys()[0], tableMetaData.getPrimaryKeys()[0]);
    }

}