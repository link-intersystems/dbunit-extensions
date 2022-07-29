package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.util.concurrent.ProgressListener;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class NullProgressListener implements ProgressListener {

    public static final ProgressListener INSTANCE = new NullProgressListener();

    @Override
    public void begin(int totalWork) {
    }

    @Override
    public void worked(int work) {
    }

    @Override
    public void done() {
    }
}
