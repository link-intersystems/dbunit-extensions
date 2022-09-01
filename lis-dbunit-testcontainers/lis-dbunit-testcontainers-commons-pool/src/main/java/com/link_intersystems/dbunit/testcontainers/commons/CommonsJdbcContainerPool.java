package com.link_intersystems.dbunit.testcontainers.commons;

import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.pool.JdbcContainerPool;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.dbunit.dataset.DataSetException;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CommonsJdbcContainerPool implements JdbcContainerPool {
    public static CommonsJdbcContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier) {
        return createPool(dBunitJdbcContainerSupplier, 1);
    }

    public static CommonsJdbcContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, int containerCount) {
        return createPool(dBunitJdbcContainerSupplier, containerCount, containerCount);
    }

    public static CommonsJdbcContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, int minContainerCount, int maxConainerCount) {
        GenericObjectPoolConfig<DBunitJdbcContainer> conf = new GenericObjectPoolConfig<>();

        conf.setMinIdle(minContainerCount);
        conf.setMaxTotal(maxConainerCount);
        conf.setTestOnBorrow(true);
        conf.setBlockWhenExhausted(true);
        conf.setMaxWait(Duration.of(5, MINUTES));

        return createPool(dBunitJdbcContainerSupplier, conf);
    }

    public static CommonsJdbcContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, GenericObjectPoolConfig<DBunitJdbcContainer> conf) {
        return createPool(dBunitJdbcContainerSupplier, pool -> {
            pool.setConfig(conf);
            pool.setTimeBetweenEvictionRuns(Duration.ofMillis(500));
        });
    }

    public static CommonsJdbcContainerPool createPool(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier, Consumer<GenericObjectPool<DBunitJdbcContainer>> poolCustomizer) {
        RunningContainerFactory runningContainerFactory = new RunningContainerFactory(dBunitJdbcContainerSupplier);
        GenericObjectPool objectPool = new GenericObjectPool<>(runningContainerFactory);

        poolCustomizer.accept(objectPool);

        return new CommonsJdbcContainerPool(objectPool);
    }

    private ObjectPool<DBunitJdbcContainer> objectPool;

    public CommonsJdbcContainerPool(ObjectPool<DBunitJdbcContainer> runningContainerObjectPool) {
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
            objectPool.returnObject((DBunitJdbcContainer) jdbcContainer);
            objectPool.invalidateObject((DBunitJdbcContainer) jdbcContainer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        objectPool.close();
    }

    private static class RunningContainerFactory implements PooledObjectFactory<DBunitJdbcContainer> {

        private Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier;

        public RunningContainerFactory(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier) {
            this.dBunitJdbcContainerSupplier = dBunitJdbcContainerSupplier;
        }

        @Override
        public void activateObject(PooledObject<DBunitJdbcContainer> p) throws DataSetException {
            DBunitJdbcContainer dBunitJdbcContainer = p.getObject();
            dBunitJdbcContainer.start();
        }

        @Override
        public void destroyObject(PooledObject<DBunitJdbcContainer> p) {
            DBunitJdbcContainer dBunitJdbcContainer = p.getObject();
            dBunitJdbcContainer.stop();
        }

        @Override
        public PooledObject<DBunitJdbcContainer> makeObject() {
            DBunitJdbcContainer dBunitJdbcContainer = dBunitJdbcContainerSupplier.get();
            return new DefaultPooledObject<>(dBunitJdbcContainer);
        }

        @Override
        public void passivateObject(PooledObject<DBunitJdbcContainer> p) {
        }

        @Override
        public boolean validateObject(PooledObject<DBunitJdbcContainer> p) {
            DBunitJdbcContainer dBunitJdbcContainer = p.getObject();
            return !dBunitJdbcContainer.isStopped();
        }
    }
}
