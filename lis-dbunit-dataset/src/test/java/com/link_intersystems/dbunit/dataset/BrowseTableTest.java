package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.browser.BrowseTable;
import org.junit.jupiter.api.Test;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BrowseTableTest {



    @Test
    void test() {
        BrowseTable actorRef = new BrowseTable("actor");
        actorRef.with("actor_id").eq(1);
        BrowseTable filmActor = actorRef.browse("film_actor").natural();
        filmActor.browse("film").natural();
        BrowseTable inventory = filmActor.browse("inventory").on("film_id").references("film_id");
        inventory.browse("store");


    }

}