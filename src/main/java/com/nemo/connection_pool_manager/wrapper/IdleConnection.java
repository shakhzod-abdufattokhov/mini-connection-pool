package com.nemo.connection_pool_manager.wrapper;

import lombok.Getter;

import java.sql.Connection;

@Getter
public class IdleConnection {
    final Connection connection;
    volatile long idleSince;

    public IdleConnection(Connection connection) {
        this.connection = connection;
        idleSince = System.currentTimeMillis();
    }
}
