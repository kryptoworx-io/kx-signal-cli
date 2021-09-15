package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLConnectionFactory {
    Connection get() throws SQLException;
}
