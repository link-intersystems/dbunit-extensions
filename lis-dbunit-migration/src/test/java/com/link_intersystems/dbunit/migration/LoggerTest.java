package com.link_intersystems.dbunit.migration;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class LoggerTest {

    @Test
    void test(){
        Logger logger = LoggerFactory.getLogger(LoggerTest.class);
        String message = "test";
        logger.info("\u2714\ufe0e {}", message);
        logger.error("\u274c {}", message);
    }
}
