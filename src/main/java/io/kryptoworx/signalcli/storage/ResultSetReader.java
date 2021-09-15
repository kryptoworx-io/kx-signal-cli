package io.kryptoworx.signalcli.storage;

import java.sql.ResultSet;
import java.sql.SQLException;

interface ResultSetReader<T> {
    T read(ResultSet rs) throws SQLException;
}
