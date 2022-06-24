package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;

import javax.activation.MimeType;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BrowseTableSerdes {

    void serialize(BrowseTable browseTable, OutputStream outputStream) throws Exception;

    BrowseTable deserialize(InputStream inputStream) throws Exception;
}
