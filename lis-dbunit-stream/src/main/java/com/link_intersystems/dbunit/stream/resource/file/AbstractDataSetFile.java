package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
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

    private File file;

    public AbstractDataSetFile(File file) {
        this.file = file;
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public IDataSetProducer createProducer() throws DataSetException {
        DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
        try {
            setProducer(producerSupport, file);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
        return producerSupport.getDataSetProducer();
    }

    protected abstract void setProducer(DataSetProducerSupport producerSupport, File file) throws IOException;

    @Override
    public IDataSetConsumer createConsumer() throws DataSetException {
        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        try {
            File parentFile = file.getParentFile();
            if (parentFile != null && !parentFile.exists()) {
                parentFile.mkdirs();
            }
            setConsumer(consumerSupport, file);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
        return consumerSupport.getDataSetConsumer();
    }

    protected abstract void setConsumer(DataSetConsumerSupport consumerSupport, File file) throws IOException;

    @Override
    public DataSetFile withNewFile(File newFile) throws DataSetException {
        try {
            Constructor<? extends AbstractDataSetFile> constructor = getClass().getDeclaredConstructor(File.class);
            constructor.setAccessible(true);
            return constructor.newInstance(newFile);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public String toString() {
        return file.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractDataSetFile that = (AbstractDataSetFile) o;
        return Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file);
    }
}
