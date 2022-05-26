package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import org.junit.jupiter.api.Test;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TableBrowseNodeTest {



    @Test
    void test() {
        TableBrowseRef actorRef = new TableBrowseRef("actor");
        actorRef.with("actor_id").eq(1);
        TableBrowseRef filmActor = actorRef.browse("film_actor").natural();
        filmActor.browse("film").natural();
        TableBrowseRef inventory = filmActor.browse("inventory").on("film_id").references("film_id");
        inventory.browse("store");

    }

}