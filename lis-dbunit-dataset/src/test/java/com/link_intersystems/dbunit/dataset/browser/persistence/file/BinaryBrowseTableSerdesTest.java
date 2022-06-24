package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BinaryBrowseTableSerdesTest {

    private BrowseTable filmActor;
    private BinaryBrowseTableSerdes serdes;

    @BeforeEach
    void setUp() {
        filmActor = new BrowseTable("film_actor");
        filmActor.with("film_id").eq(1);
        BrowseTable actor = filmActor.browse("actor").natural();
        actor.with("first_name").like("P%");
        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").on("language_id").references("language_id");

        serdes = new BinaryBrowseTableSerdes();
    }

    @Test
    void serdesNull() throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        serdes.serialize(null, bout);

        BrowseTable deserialized = serdes.deserialize(new ByteArrayInputStream(bout.toByteArray()));

        assertNull(deserialized);
    }

    @Test
    void serdes() throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        serdes.serialize(filmActor, bout);

        BrowseTable deserialized = serdes.deserialize(new ByteArrayInputStream(bout.toByteArray()));

        assertEquals(filmActor, deserialized);
    }
}