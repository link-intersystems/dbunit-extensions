package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;

import java.lang.ref.WeakReference;
import java.text.MessageFormat;
import java.util.EmptyStackException;
import java.util.Stack;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcContainerHolder {

    private static class JdbcContainerHolderUnavailableException extends EmptyStackException {


        private String msg;

        JdbcContainerHolderUnavailableException(String msg) {
            this.msg = msg;
        }

        @Override
        public String getMessage() {
            return msg;
        }
    }


    private static ThreadLocal<Stack<WeakReference<JdbcContainer>>> HOLDER = ThreadLocal.withInitial(() -> new Stack<>());

    public static JdbcContainer get() {
        Stack<WeakReference<JdbcContainer>> stack = HOLDER.get();

        if (stack.isEmpty()) {
            String msg = MessageFormat.format("No ''{0}'' available.", JdbcContainer.class.getSimpleName());
            throw new JdbcContainerHolderUnavailableException(msg);
        }

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
