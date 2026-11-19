package com.nemo.connection_pool_manager;

import com.nemo.connection_pool_manager.wrapper.IdleConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionPool {
    private final PoolConfig config;
    private final BlockingQueue<IdleConnection> idleConnection;
    private AtomicInteger totalCreated = new AtomicInteger(0);
    private final ScheduledThreadPoolExecutor maintenanceExecutor = createExecutor();

    private final AtomicInteger leaksDetected = new AtomicInteger();

    private static ScheduledThreadPoolExecutor createExecutor() {
        ScheduledThreadPoolExecutor scheduledExecutorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread th = new Thread(r, "mini-pool-maintenance");
            th.setDaemon(true);
            return th;
        });
        scheduledExecutorService.setRemoveOnCancelPolicy(true);
        return scheduledExecutorService;
    }

    public ConnectionPool(PoolConfig config) throws SQLException {
        this.config = config;
        idleConnection = new LinkedBlockingDeque<>(config.maxPoolSize());
        fillConnectionPool();

        maintenanceExecutor.scheduleAtFixedRate(
                this::evictIdleConnection, 30, 30, TimeUnit.MILLISECONDS);
    }

    public Connection borrow() throws SQLException {
        Connection raw = acquire();
        LeakTask leakTask = LeakTask.schedule(maintenanceExecutor, config.connectionTimeoutMills(), leaksDetected);

        return PooledConnectionHandler.wrap(raw, this, leakTask);
    }

    int leaksDetected() {
        return leaksDetected.get();
    }

    private Connection acquire() throws SQLException {
        IdleConnection idle = idleConnection.poll();
        if(idle != null) {
            return idle.getConnection();
        }

        if(totalCreated.get() < config.maxPoolSize()) {
            synchronized (this) {
                if (totalCreated.get() < config.maxPoolSize()) {
                    totalCreated.incrementAndGet();
                    return createConnection();
                }
            }
        }

        try{
             idle = idleConnection.poll(config.connectionTimeoutMills(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection",e);
        }

        if(idle == null) {
            throw new SQLException("Timeout waiting for connection from pool");
        }

        return idle.getConnection();
    }

    public void release(Connection raw) {
        idleConnection.offer(new IdleConnection(raw));
    }

    public void evictIdleConnection() {
        long now = System.currentTimeMillis();
        int excess = idleConnection.size() - config.minIdle();
        if(excess <= 0) return;

        List<IdleConnection> toCheck = new ArrayList<>();
        idleConnection.drainTo(toCheck);

        for (IdleConnection idle : toCheck) {
            boolean tooOld = (now - idle.getIdleSince()) >= config.connectionTimeoutMills();
            boolean overMinIdle = toCheck.size() > config.minIdle();

            if(tooOld && overMinIdle) {
                totalCreated.decrementAndGet();
                closeQuitely(idle.getConnection());
            } else {
                idleConnection.offer(idle);
            }
        }
    }

    public void closeQuitely(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection createConnection() throws SQLException {
        return DriverManager.getConnection(
                config.jdbcUrl(), config.username(), config.password()
        );
    }

    public void shutDown() {
        maintenanceExecutor.shutdown();
        idleConnection.forEach(idle -> closeQuitely(idle.getConnection()));
    }

    public int idleCount(){
        return totalCreated.get();
    }

    public void fillConnectionPool() throws SQLException {
        for(int i = 0; i<config.minIdle(); i++) {
            totalCreated.incrementAndGet();
            idleConnection.offer(new IdleConnection(createConnection()));
        }
    }

    public void discard(Connection raw) {
        closeQuitely(raw);
        totalCreated.decrementAndGet();
    }

}
