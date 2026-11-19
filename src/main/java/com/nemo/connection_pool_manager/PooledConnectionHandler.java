package com.nemo.connection_pool_manager;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

public class PooledConnectionHandler implements InvocationHandler {
    private final ConnectionPool pool;
    private final Connection real;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LeakTask leakTask;

    private PooledConnectionHandler(ConnectionPool pool, Connection real, LeakTask leakTask) {
        this.pool = pool;
        this.real = real;
        this.leakTask = leakTask;
    }

    public static Connection wrap(Connection real, ConnectionPool pool, LeakTask leakTask) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                new PooledConnectionHandler(pool, real, leakTask));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        switch (method.getName()) {
            case "close" -> {
                if(closed.compareAndSet(false, true)){
                    if(leakTask != null) leakTask.cancel();
                    returnToPoll();
                }
                return  null;
            }
            case "isClosed" -> {return closed.get() || real.isClosed(); }
            case "equals" -> {return proxy == args[0]; }
            case "hashCode" -> {return System.identityHashCode(proxy); }
            case "toString" -> {return "PooledConnection[" + real + "}"; }
        }

        if(closed.get()) {
            throw new SQLException("Connection has already been returned to the pool");
        }

        try {
            return method.invoke(real, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private void returnToPoll() {
        try {
            if(!real.getAutoCommit()) {
                real.rollback();
                real.setAutoCommit(true);
            }

            pool.release(real);
        } catch (SQLException e) {
            pool.discard(real);
        }
    }
}
