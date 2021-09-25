package org.asamk.signal.manager.storage.recipients;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.codec.binary.Hex;
import org.h2.jdbcx.JdbcConnectionPool;

public class H2Test {
	
	protected static SecureRandom random = new SecureRandom();
	
	protected static class TestDataSource implements DataSource, AutoCloseable {
		private final JdbcConnectionPool pool;
		private final Path dbFile;

		TestDataSource(Path dbFile) {
			this.dbFile = dbFile;
			String databaseUrl = "jdbc:h2:file:" + dbFile + ";cipher=AES";
			byte[] dbKey = new byte[32];
			random.nextBytes(dbKey);
			pool = JdbcConnectionPool.create(databaseUrl, "", Hex.encodeHexString(dbKey) + " ");
			pool.setMaxConnections(4);
			dbFile.toFile().deleteOnExit();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return pool.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return pool.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return pool.isWrapperFor(iface);
		}

		@Override
		public void close() throws IOException {
			pool.dispose();
			String glob = dbFile.getFileName() + "*";
			try (var dir = Files.newDirectoryStream(dbFile.getParent(), glob)) {
				var it = dir.iterator();
				while (it.hasNext()) {
					Files.delete(it.next());
				}
			}
		}

		@Override
		public Connection getConnection() throws SQLException {
			Connection c = pool.getConnection();
			c.setAutoCommit(false);
			return c;
		}

		@Override
		public Connection getConnection(String username, String password) throws SQLException {
			Connection c = pool.getConnection(username, password);
			c.setAutoCommit(false);
			return c;
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return pool.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			pool.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			pool.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return pool.getLoginTimeout();
		}
	}
	
	protected static TestDataSource createDataSource() throws IOException {
		Path dbFile = Files.createTempFile("test", "");
		return new TestDataSource(dbFile);
	}
}
