package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;

public interface SQLConnectionFactory extends AutoCloseable {
    Connection getConnection() throws SQLException;
    default void close() throws SQLException { }
}
