package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.persistence.ModelPersistenceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */

class FileBrowseModelRepositoryTest {
    private BrowseTable filmActor;
    private FileBrowseModelRepository repository;

    @BeforeEach
    void setUp(@TempDir File tempDir) {
        filmActor = new BrowseTable("film_actor");
        filmActor.with("film_id").eq(1);
        BrowseTable actor = filmActor.browse("actor").natural();
        actor.with("first_name").like("P%");
        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").on("language_id").references("language_id");


        repository = new FileBrowseModelRepository(tempDir, new BinaryBrowseTableSerdes(), "bin");
    }
    @Test
    void persistAndLoad() throws ModelPersistenceException {

        repository.persistModel(filmActor, "testmodel");

        BrowseTable testmodel = repository.loadModel("testmodel");

        assertNotSame(filmActor, testmodel);
        assertEquals(filmActor, testmodel);
    }

}