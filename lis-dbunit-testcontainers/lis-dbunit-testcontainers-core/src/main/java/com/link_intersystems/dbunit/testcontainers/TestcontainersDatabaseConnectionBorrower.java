package com.link_intersystems.dbunit.testcontainers;

import com.link_intersystems.dbunit.database.DatabaseConnectionBorrower;
import com.link_intersystems.dbunit.database.DatabaseConnectionDelegate;
import com.link_intersystems.dbunit.testcontainers.pool.JdbcContainerPool;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;

import java.text.MessageFormat;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestcontainersDatabaseConnectionBorrower extends DatabaseConnectionBorrower {

    private static class ContainerAwareDatabaseConnection extends DatabaseConnectionDelegate {

        private JdbcContainer jdbcContainer;

        public ContainerAwareDatabaseConnection(JdbcContainer jdbcContainer) {
            this.jdbcContainer = requireNonNull(jdbcContainer);
        }

        @Override
        protected IDatabaseConnection getTargetConnection() {
            return jdbcContainer.getDatabaseConnection();
        }
    }

    private JdbcContainerSetup jdbcContainerSetup;
    private JdbcContainerPool jdbcContainerPool;

    public TestcontainersDatabaseConnectionBorrower(JdbcContainerPool jdbcContainerPool, JdbcContainerSetup jdbcContainerSetup) {
        this.jdbcContainerPool = requireNonNull(jdbcContainerPool);
        this.jdbcContainerSetup = requireNonNull(jdbcContainerSetup);
    }

    @Override
    protected IDatabaseConnection borrowTargetConnection() throws DatabaseUnitException {
        JdbcContainer jdbcContainer = jdbcContainerPool.borrowContainer();
        try {
            jdbcContainerSetup.setup(jdbcContainer);
        } catch (DatabaseUnitException e) {
            throw e;
        } catch (Exception e) {
            throw new DatabaseUnitException(e);
        }
        return new ContainerAwareDatabaseConnection(jdbcContainer);
    }

    @Override
    protected void returnTargetConnection(IDatabaseConnection targetConnection) throws DatabaseUnitException {
        if (!(targetConnection instanceof ContainerAwareDatabaseConnection)) {
            String msg = MessageFormat.format("targetConnection was not created by this {0}", TestcontainersDatabaseConnectionBorrower.class);
            throw new IllegalArgumentException(msg);
        }
        super.returnTargetConnection(targetConnection);
        ContainerAwareDatabaseConnection containerAwareDatabaseConnection = (ContainerAwareDatabaseConnection) targetConnection;
        jdbcContainerPool.returnContainer(containerAwareDatabaseConnection.jdbcContainer);
    }
}
