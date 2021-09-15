package io.kryptoworx.signalcli.storage;

import java.sql.SQLException;

@FunctionalInterface 
public interface SQLSupplier<T, E extends Exception> {
    T get() throws SQLException, E;
}