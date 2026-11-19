package com.nemo.connection_pool_manager;

public interface PoolConfig {
    String jdbcUrl();
    String username();
    String password();
    int minIdle();
    int maxPoolSize();
    long connectionTimeoutMills();
}
