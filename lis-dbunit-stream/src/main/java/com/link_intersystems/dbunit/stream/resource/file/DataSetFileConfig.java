package com.link_intersystems.dbunit.stream.resource.file;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileConfig {

    private Charset charset = StandardCharsets.UTF_8;
    private boolean columnSensing;

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = requireNonNull(charset);
    }

    public boolean isColumnSensing() {
        return columnSensing;
    }

    public void setColumnSensing(boolean columnSensing) {
        this.columnSensing = columnSensing;
    }
}
