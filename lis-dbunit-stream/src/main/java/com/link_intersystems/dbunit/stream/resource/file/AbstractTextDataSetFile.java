package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.producer.AutocloseDataSetProducer;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.*;
import java.nio.charset.Charset;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractTextDataSetFile extends AbstractDataSetFile {

    private Charset charset;

    public AbstractTextDataSetFile(File file) {
        super(file);
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public Charset getCharset() {
        return charset;
    }

    @Override
    protected final void setProducer(DataSetProducerSupport producerSupport, File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        Charset charset = getCharset();
        InputStreamReader characterStream = new InputStreamReader(new BufferedInputStream(inputStream), charset);
        IDataSetProducer dataSetProducer = createProducer(characterStream);
        AutocloseDataSetProducer autocloseDataSetProducer = new AutocloseDataSetProducer(dataSetProducer, inputStream);
        producerSupport.setDataSetProducer(autocloseDataSetProducer);
    }

    @Override
    public DataSetFile withNewFile(File newFile) throws DataSetException {
        AbstractTextDataSetFile dataSetFile = (AbstractTextDataSetFile) super.withNewFile(newFile);
        dataSetFile.setCharset(getCharset());
        return dataSetFile;
    }

    protected abstract IDataSetProducer createProducer(Reader reader);
}
