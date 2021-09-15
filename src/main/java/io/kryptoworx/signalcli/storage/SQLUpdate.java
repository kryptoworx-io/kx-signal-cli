package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SQLUpdate {

    private final List<SQLArgument<?>> args = new ArrayList<>();
    private final StringBuilder sqlBuilder;
    private String whereCondition;
    private SQLArgument<?>[] whereConditionArgs;
    private final String tableAlias;

    public static SQLUpdate forTable(String table) {
        return forTable(table, "t");
    }

    public static SQLUpdate forTable(String table, String tableAlias) {
        return new SQLUpdate(table, tableAlias);
    }

    private SQLUpdate(String table, String tableAlias) {
        this.tableAlias = tableAlias;
        this.sqlBuilder = new StringBuilder("UPDATE ")
                .append(table)
                .append(' ')
                .append(tableAlias)
                .append(" SET ");
    }

    public <T> SQLUpdate set(String column, SQLArgument<T> value) {
        if (!args.isEmpty()) sqlBuilder.append(", ");
        sqlBuilder
                .append(tableAlias)
                .append('.')
                .append(column)
                .append(" = ?");
        args.add(value);
        return this;
    }

    public SQLUpdate where(String whereCondition, SQLArgument<?>... args) {
        this.whereCondition = whereCondition;
        this.whereConditionArgs = args;
        return this;
    }

    public void execute(Connection connection) throws SQLException {
        execute(connection, null);
    }

    @SuppressWarnings("rawtypes")
    public <T> void execute(Connection connection, ResultSetReader<T> keyReader) throws SQLException {
        if (whereCondition != null) {
            sqlBuilder.append(" WHERE ").append(whereCondition);
        }

        try (PreparedStatement stmt = connection.prepareStatement(sqlBuilder.toString())) {
            int p = 1;
            for (SQLArgument v : args) {
                v.set(stmt, p++);
            }
            if (whereConditionArgs != null) {
                for (SQLArgument v : whereConditionArgs) {
                    v.set(stmt, p++);
                }
            }
            stmt.executeUpdate();
        }
    }
}
