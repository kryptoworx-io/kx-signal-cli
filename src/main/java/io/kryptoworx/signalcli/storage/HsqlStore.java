package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HsqlStore implements AutoCloseable {
    
    private final static Logger logger = LoggerFactory.getLogger(HsqlStore.class);
    
    private static final ThreadLocal<Connection> txConnection = new ThreadLocal<>();

    private static class DatabaseException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private DatabaseException(SQLException cause) {
            super(cause);
        }
        
        @Override
        public synchronized SQLException getCause() {
            return (SQLException) super.getCause();
        }
    }
    
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
    
    protected <E extends Exception> void voidTransaction(SQLConnectionRunnable<E> runnable) throws E {
        SQLConnectionCallable<Void, E> callable = connection -> {
            runnable.run(connection);
            return null;
        };
        transaction(callable);
    }
    
    protected <T, E extends Exception> T transaction(SQLConnectionCallable<T, E> callable) throws E {
        Connection connection = txConnection.get();
        if (connection != null) {
            try {
                return callable.call(connection);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        } else {
            return transaction(getConnection(connectionFactory), callable);
        }
    }

    public static <E extends Exception> void transaction(SQLConnectionFactory connectionFactory, Runnable r) throws E {
        Connection connection = txConnection.get();
        if (connection == null) {
            connection = getConnection(connectionFactory);
            txConnection.set(connection);
        }    
        
        try {
            transaction(connection, c -> { r.run(); return null; });
        } finally {
            txConnection.remove();
        }
    }
    
    private static <T, E extends Exception> T transaction(Connection connection, SQLConnectionCallable<T, E> callable) throws E {
        try {
            T result = callable.call(connection);
            connection.commit();
            return result;
        } catch (SQLException | DatabaseException e) {
            Throwable cause = e;
            if (e instanceof DatabaseException dbe) {
                cause = dbe.getCause();
            }
            System.out.println("DEBUG> ");
            e.printStackTrace();
            logger.error("Unexpected SQL failure", cause);
            rollback(connection);
            throw new AssertionError("Unexceped SQL failure", cause);
        } catch (Exception e) {
            rollback(connection);
            throw e;
        } finally {
            close(connection);
        }
    }
    
    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            logger.error("Failed to rollback transaction", e);
        }
    }

    private static void close(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error("Failed to close database connection", e);
        }
    }

    protected Connection getConnection() throws SQLException {
        return connectionFactory.getConnection();
    }

    private static Connection getConnection(SQLConnectionFactory connectionFactory) {
        try {
            Connection connection = connectionFactory.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new AssertionError("Failed to acquire database connection", e);
        }
    }
    
    @Override
    public void close() throws SQLException {
        connectionFactory.close();
    }
}
