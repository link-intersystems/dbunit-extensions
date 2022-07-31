package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.producer.CloseableDataSetProducer;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
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
        InputStreamReader characterStream = new InputStreamReader(new BufferedInputStream(inputStream), getCharset());
        IDataSetProducer dataSetProducer = createProducer(characterStream);
        CloseableDataSetProducer closeableDataSetProducer = new CloseableDataSetProducer(dataSetProducer, characterStream);
        producerSupport.setDataSetProducer(closeableDataSetProducer);
    }

    protected abstract IDataSetProducer createProducer(Reader reader);
}
