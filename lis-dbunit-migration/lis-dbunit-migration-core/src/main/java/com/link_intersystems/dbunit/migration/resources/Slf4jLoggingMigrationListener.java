package com.link_intersystems.dbunit.migration.resources;

import org.dbunit.dataset.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Slf4jLoggingMigrationListener extends AbstractLoggingMigrationListener {

    private Logger logger;

    public Slf4jLoggingMigrationListener() {
        this(LoggerFactory.getLogger(Slf4jLoggingMigrationListener.class));
    }

    protected Logger getLogger() {
        return logger;
    }

    public Slf4jLoggingMigrationListener(Logger logger) {
        this.logger = requireNonNull(logger);
    }

    @Override
    protected void logMigrationSuccessful(String msg) {
        getLogger().info(msg);
    }

    @Override
    protected void logMigrationFailed(String msg, DataSetException e) {
        Logger logger = getLogger();
        if (logger.isDebugEnabled()) {
            logger.error(msg, e);
        } else {
            logger.error(msg);
        }
    }

    @Override
    protected void logStartMigration(String msg) {
        getLogger().info(msg);
    }

    @Override
    protected void logMigrationsFinished(String msg, Supplier<String> details) {
        Logger logger = getLogger();
        logger.info(msg);
        if (logger.isDebugEnabled()) {
            logger.debug(details.get());
        }
    }


}
