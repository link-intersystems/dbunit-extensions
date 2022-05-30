package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.test.db.sakila.SakilaEmptyTestDBExtension;
import com.link_intersystems.test.db.sakila.SakilaSlimTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaEmptyTestDBExtension.class)
class BrowseTableTest {

    private ConnectionMetaData connectionMetaData;

    @BeforeEach
    void setUp(Connection sakilaConnection) {
        connectionMetaData = new ConnectionMetaData(sakilaConnection);
    }

    @Test
    void makeConsistent() throws SQLException {
        BrowseTable filmActor = new BrowseTable("film_actor");
        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").on("original_language_id").references("language_id");

        filmActor.makeConsistent(connectionMetaData);

        BrowseTableAssertions filmActorAssertions = BrowseTableAssertions.notNull(filmActor);

        BrowseTableAssertions filmAssertions = filmActorAssertions.assertReferences("film");
        filmAssertions.assertReferencesByColumns("language")
                .on("original_language_id")
                .references("language_id");
        filmAssertions.assertReferencesByColumns("language")
                .on("language_id")
                .references("language_id");

        BrowseTableAssertions actorAssertions = filmActorAssertions.assertReferences("actor");
        actorAssertions.assertEmptyReferences();

    }
}