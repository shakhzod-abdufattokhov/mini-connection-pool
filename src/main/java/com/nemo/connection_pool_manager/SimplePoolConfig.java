package com.nemo.connection_pool_manager;


public class SimplePoolConfig implements PoolConfig{

    public SimplePoolConfig(String jdbcUrl, String username, String password, int minIdle, int maxPoolSize, long connTimeoutMills){
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.minIdle = minIdle;
        this.maxPoolSize = maxPoolSize;
        this.connectionTimeoutMills = connTimeoutMills;
    }

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final int minIdle;
    private final int maxPoolSize;
    private final long connectionTimeoutMills;

    @Override
    public String jdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public int minIdle() {
        return minIdle;
    }

    @Override
    public int maxPoolSize() {
        return maxPoolSize;
    }

    @Override
    public long connectionTimeoutMills() {
        return connectionTimeoutMills;
    }
}
