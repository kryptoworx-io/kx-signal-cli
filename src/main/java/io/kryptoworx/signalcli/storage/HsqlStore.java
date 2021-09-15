package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HsqlStore {
    
    private final static Logger logger = LoggerFactory.getLogger(HsqlStore.class);
    
    @FunctionalInterface
    protected interface SQLRunnable<E extends Exception> {
        void run() throws SQLException, E;
    }

    @FunctionalInterface
    protected interface SQLConnectionRunnable<E extends Exception> {
        void run(Connection c) throws SQLException, E;
    }

    @FunctionalInterface 
    public interface SQLConnectionCallable<T, E extends Exception> {
        T call(Connection c) throws SQLException, E;
    }
    
    private final SQLConnectionFactory connectionFactory;
    
    HsqlStore(SQLConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }
    
    /*
    protected Connection connection(boolean autoCommit) throws SQLException {
        Connection connection = connectionFactory.get();
        connection.setAutoCommit(autoCommit);
        return connection;
    }
    */

    protected <E extends Exception> void voidTransaction(SQLConnectionRunnable<E> runnable) throws E {
        SQLConnectionCallable<Void, E> callable = connection -> {
            runnable.run(connection);
            return null;
        };
        transaction(callable);
    }
    
    protected <T, E extends Exception> T transaction(SQLConnectionCallable<T, E> callable) throws E {
        try (Connection connection = connectionFactory.get()){
            connection.setAutoCommit(false);
            try {
                T result = callable.call(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                rollback(connection);
                throw e;
            }
        } catch (SQLException e) {
            logger.error("Unexpected SQL failure", e);
            throw new AssertionError("Unexceped SQL failure", e);
        }
    }
    
    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            logger.error("Rollback failed", e);
        }
    }
}
