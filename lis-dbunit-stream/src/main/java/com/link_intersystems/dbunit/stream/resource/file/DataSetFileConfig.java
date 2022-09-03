package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.util.config.properties.ConfigProperty;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileConfig {
    public static final ConfigProperty<Charset> CHARSET = ConfigProperty.named("charset").withDefaultValue(StandardCharsets.UTF_8);

    public Charset getCharset();
}
