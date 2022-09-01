package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;

import java.lang.ref.WeakReference;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcContainerHolder {

    private static ThreadLocal<WeakReference<JdbcContainer>> HOLDER = new ThreadLocal();


    public static JdbcContainer get() {
        WeakReference<JdbcContainer> reference = HOLDER.get();
        if (reference == null) {
            return null;
        }
        return reference.get();
    }

    public static void set(JdbcContainer jdbcContainer) {
        HOLDER.set(new WeakReference<>(jdbcContainer));
    }


    public static void remove() {
        HOLDER.remove();
    }
}
