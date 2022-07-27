package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractDataSetFile implements DataSetFile {

    private FilePath filePath;

    public AbstractDataSetFile(FilePath filePath) {
        this.filePath = filePath;
    }

    @Override
    public IDataSetProducer createProducer() throws DataSetException {
        DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
        try {
            setProducer(producerSupport, filePath);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
        return producerSupport.getDataSetProducer();
    }

    protected abstract void setProducer(DataSetProducerSupport producerSupport, FilePath filePath) throws IOException;

    @Override
    public IDataSetConsumer createConsumer() throws DataSetException {
        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        try {
            File absoluteFile = filePath.toAbsoluteFile();
            File parentFile = absoluteFile.getParentFile();
            if (parentFile != null && !parentFile.exists()) {
                parentFile.mkdirs();
            }
            setConsumer(consumerSupport, filePath);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
        return consumerSupport.getDataSetConsumer();
    }

    protected abstract void setConsumer(DataSetConsumerSupport consumerSupport, FilePath filePath) throws IOException;

    @Override
    public DataSetFile withNewPath(Path newBasePath) throws DataSetException {
        try {
            Constructor<? extends AbstractDataSetFile> constructor = getClass().getDeclaredConstructor(FilePath.class);
            constructor.setAccessible(true);
            FilePath rebasedPath = filePath.rebase(newBasePath);
            return constructor.newInstance(rebasedPath);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public String toString() {
        return filePath.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractDataSetFile that = (AbstractDataSetFile) o;
        return Objects.equals(filePath, that.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath);
    }
}
