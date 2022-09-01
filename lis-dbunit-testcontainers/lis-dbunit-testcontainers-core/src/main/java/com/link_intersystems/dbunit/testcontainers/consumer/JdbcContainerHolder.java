package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;

import java.lang.ref.WeakReference;
import java.util.Stack;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcContainerHolder {


    private static ThreadLocal<Stack<WeakReference<JdbcContainer>>> HOLDER = ThreadLocal.withInitial(() -> new Stack<>());

    public static JdbcContainer get() {
        Stack<WeakReference<JdbcContainer>> stack = HOLDER.get();
        WeakReference<JdbcContainer> reference = stack.peek();
        if (reference == null) {
            return null;
        }
        return reference.get();
    }

    public static void set(JdbcContainer jdbcContainer) {
        Stack<WeakReference<JdbcContainer>> stack = HOLDER.get();
        stack.push(new WeakReference<>(jdbcContainer));
    }


    public static void remove() {
        Stack<WeakReference<JdbcContainer>> stack = HOLDER.get();
        if (!stack.isEmpty()) {
            stack.pop();
        }
    }
}
