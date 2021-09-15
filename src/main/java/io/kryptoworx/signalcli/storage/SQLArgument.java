package io.kryptoworx.signalcli.storage;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class SQLArgument<T> {
    private final T value;
    private final SQLParameterSetter<T> parameterSetter;
    
    public SQLArgument(T value, SQLParameterSetter<T> parameterSetter) {
        this.value = value;
        this.parameterSetter = parameterSetter;
    }
    
    public void set(PreparedStatement stmt, int parameterIndex) throws SQLException {
        parameterSetter.setValue(stmt, parameterIndex, value);
    }
    
    public static SQLArgument<String> of(String s) {
        return new SQLArgument<>(s, PreparedStatement::setString);
    }

    public static SQLArgument<Integer> of(int v) {
        return new SQLArgument<>(v, PreparedStatement::setInt);
    }

    public static SQLArgument<Long> of(long v) {
        return new SQLArgument<>(v, PreparedStatement::setLong);
    }

    public static SQLArgument<byte[]> of(byte[] v) {
        return new SQLArgument<>(v, PreparedStatement::setBytes);
    }
    
    public static SQLArgument<Boolean> of(boolean v) {
        return new SQLArgument<>(v, PreparedStatement::setBoolean);
    }

    public static SQLArgument<UUID> of(UUID uuid) {
        return new SQLArgument<>(uuid, PreparedStatement::setObject);
    }
}
