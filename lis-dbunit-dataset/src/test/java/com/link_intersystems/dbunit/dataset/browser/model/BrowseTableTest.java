package com.link_intersystems.dbunit.dataset.browser.model;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.test.db.sakila.SakilaEmptyExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaEmptyExtension
class BrowseTableTest {

    private ConnectionMetaData connectionMetaData;

    @BeforeEach
    void setUp(Connection sakilaConnection) {
        connectionMetaData = new ConnectionMetaData(sakilaConnection);
    }

    @Test
    void dslApi() {
        BrowseTable actorRef = new BrowseTable("actor");
        actorRef.with("actor_id").eq(1);
        BrowseTable filmActor = actorRef.browse("film_actor").natural();
        filmActor.browse("film").natural();
        BrowseTable inventory = filmActor.browse("inventory").on("film_id").references("film_id");
        inventory.browse("store");
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