package com.link_intersystems.dbunit.testcontainers.commons;

import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import com.link_intersystems.dbunit.testcontainers.pool.JdbcContainerPool;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CommonsRunningContainerPool implements JdbcContainerPool {
    public static CommonsRunningContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier) {
        return createPool(dBunitJdbcContainerSupplier, 1);
    }

    public static CommonsRunningContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, int containerCount) {
        return createPool(dBunitJdbcContainerSupplier, containerCount, containerCount);
    }

    public static CommonsRunningContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, int minContainerCount, int maxConainerCount) {
        GenericObjectPoolConfig<RunningContainer> conf = new GenericObjectPoolConfig<>();

        conf.setMinIdle(minContainerCount);
        conf.setMaxTotal(maxConainerCount);
        conf.setTestOnBorrow(true);
        conf.setBlockWhenExhausted(true);
        conf.setMaxWait(Duration.of(5, MINUTES));

        return createPool(dBunitJdbcContainerSupplier, conf);
    }

    public static CommonsRunningContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, GenericObjectPoolConfig<RunningContainer> conf) {
        RunningContainerFactory runningContainerFactory = new RunningContainerFactory(dBunitJdbcContainerSupplier);
        GenericObjectPool objectPool = new GenericObjectPool<>(runningContainerFactory);
        runningContainerFactory.setObjectPool(objectPool);

        return createPool(dBunitJdbcContainerSupplier, pool -> {
            pool.setConfig(conf);
            pool.setTimeBetweenEvictionRuns(Duration.ofMillis(500));
        });
    }

    public static CommonsRunningContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, Consumer<GenericObjectPool<RunningContainer>> poolCustomizer) {
        RunningContainerFactory runningContainerFactory = new RunningContainerFactory(dBunitJdbcContainerSupplier);
        GenericObjectPool objectPool = new GenericObjectPool<>(runningContainerFactory);
        runningContainerFactory.setObjectPool(objectPool);

        poolCustomizer.accept(objectPool);

        return new CommonsRunningContainerPool(objectPool);
    }

    private ObjectPool<RunningContainer> objectPool;

    public CommonsRunningContainerPool(ObjectPool<RunningContainer> runningContainerObjectPool) {
        objectPool = requireNonNull(runningContainerObjectPool);
    }

    @Override
    public JdbcContainer borrowContainer() throws DataSetException {
        try {
            return objectPool.borrowObject();
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void returnContainer(JdbcContainer jdbcContainer) {
        try {
            objectPool.returnObject((RunningContainer) jdbcContainer);
            objectPool.invalidateObject((RunningContainer) jdbcContainer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        objectPool.close();
    }

    private static class RunningContainerFactory implements PooledObjectFactory<RunningContainer> {

        private Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier;
        private ObjectPool<RunningContainer> objectPool;

        public RunningContainerFactory(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier) {
            this.dBunitJdbcContainerSupplier = dBunitJdbcContainerSupplier;
        }

        @Override
        public void activateObject(PooledObject<RunningContainer> p) {
        }

        @Override
        public void destroyObject(PooledObject<RunningContainer> p) {
            RunningContainer runningContainer = p.getObject();
            if (runningContainer instanceof RunningContainerProxy) {
                runningContainer = ((RunningContainerProxy) runningContainer).target;
            }
            runningContainer.stop();
        }

        @Override
        public PooledObject<RunningContainer> makeObject() throws Exception {
            DBunitJdbcContainer dBunitJdbcContainer = dBunitJdbcContainerSupplier.get();
            RunningContainer runningContainer = dBunitJdbcContainer.start();
            RunningContainerProxy runningContainerProxy = new RunningContainerProxy(objectPool, runningContainer);
            return new DefaultPooledObject<>(runningContainerProxy);
        }

        @Override
        public void passivateObject(PooledObject<RunningContainer> p) {
        }

        @Override
        public boolean validateObject(PooledObject<RunningContainer> p) {
            RunningContainer runningContainer = p.getObject();
            return !runningContainer.isStopped();
        }

        public void setObjectPool(ObjectPool<RunningContainer> objectPool) {
            this.objectPool = objectPool;
        }
    }

    private static class RunningContainerProxy implements RunningContainer {
        private ObjectPool<RunningContainer> objectPool;
        private RunningContainer target;

        public RunningContainerProxy(ObjectPool<RunningContainer> objectPool, RunningContainer target) {
            this.objectPool = objectPool;
            this.target = target;
        }

        @Override
        public DataSource getDataSource() {
            return target.getDataSource();
        }

        @Override
        public IDatabaseConnection getDatabaseConnection() {
            return target.getDatabaseConnection();
        }

        @Override
        public void stop() {
            try {
                objectPool.returnObject(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean isStopped() {
            return target.isStopped();
        }
    }
}
