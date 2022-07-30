package com.link_intersystems.dbunit.migration.collection;

import org.dbunit.dataset.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class LoggingDataSetCollectionMigrationListener extends AbstractLoggingDataSetCollectionMigrationListener {

    private Logger logger;

    public LoggingDataSetCollectionMigrationListener() {
        this(LoggerFactory.getLogger(LoggingDataSetCollectionMigrationListener.class));

    }

    public LoggingDataSetCollectionMigrationListener(Logger logger) {
        this.logger = requireNonNull(logger);
    }

    @Override
    protected void logSucessfullyMigrated(String msg) {
        logger.info(msg);
    }

    @Override
    protected void logFailedMigration(DataSetException e, String msg) {
        if (logger.isDebugEnabled()) {
            logger.error(msg, e);
        } else {
            logger.error(msg);
        }
    }

    @Override
    protected void logStartMigration(String msg) {
        logger.info(msg);
    }

    @Override
    protected void logResourcesSupplied(String msg, Supplier<String> details) {
        logger.info(msg);
        if (logger.isDebugEnabled()) {
            logger.debug(details.get());
        }
    }

    @Override
    protected void logDataSetCollectionMigrationFinished(String msg, Supplier<String> details) {
        logger.info(msg);
        if (logger.isDebugEnabled()) {
            logger.debug(details.get());
        }
    }
}
