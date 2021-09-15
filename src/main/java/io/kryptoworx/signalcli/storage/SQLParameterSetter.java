package io.kryptoworx.signalcli.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLParameterSetter<T> {
    void setValue(PreparedStatement stmt, int parameterIndex, T value) throws SQLException;
}
