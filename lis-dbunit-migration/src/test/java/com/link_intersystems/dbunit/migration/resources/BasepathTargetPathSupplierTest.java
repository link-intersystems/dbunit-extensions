package com.link_intersystems.dbunit.migration.resources;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BasepathTargetPathSupplierTest {

    @Test
    void getTarget() {
        BasepathTargetPathSupplier targetPathSupplier = new BasepathTargetPathSupplier(Paths.get("/source"), Paths.get("/target"));


    }
}