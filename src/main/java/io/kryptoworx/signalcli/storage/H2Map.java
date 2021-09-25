package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class H2Map<K, V> implements AutoCloseable {

    private final static Logger LOGGER = LoggerFactory.getLogger(H2Map.class);
	
	private final BiFunction<K, V, byte[]> serializer;
	private final BiFunction<K, byte[], V> deserializer;
	private final String tableName;
	private final Column<K> primaryKeyColumn;
	private final Column<?>[] indexColumns;
	private final String sqlInsert;
	private final DataSource dataSource;
	
	@FunctionalInterface
	public interface PreparedStatementSetter<T> {
		void setValue(PreparedStatement stmt, int parameterIndex, T value) throws SQLException;
	}

	@FunctionalInterface
	public interface ResultSetGetter<T> {
		T getValue(ResultSet stmt, int columnIndex) throws SQLException;
	}
	
	@FunctionalInterface
	protected interface ConnectionRunnable {
		void run(Connection c) throws SQLException;
	}

	@FunctionalInterface
	protected interface ConnectionFunction<T> {
		T run(Connection c) throws SQLException;
	}
	
	protected static class MutableValue<V> implements Consumer<V> {
		private V value;
		
		public MutableValue() {
			
		}

		@Override
		public void accept(V v) {
			value = v;
		}
		
		public V get() {
			return value;
		}
	}
	
	public static record Column<T>(String name, 
			String sqlType, 
			PreparedStatementSetter<T> setter,
			ResultSetGetter<T> getter) { }
	
	protected H2Map(DataSource dataSource, String tableName,
			BiFunction<K, V, byte[]> serializer, BiFunction<K, byte[], V> deserializer) {
		this.dataSource = dataSource;
		this.tableName = tableName;
		this.serializer = serializer;
		this.deserializer = deserializer;
		this.primaryKeyColumn = createPrimaryKeyColumn();
		this.indexColumns = createIndexColumns();
		this.sqlInsert = createSqlInsert();
		voidTransaction(this::createTableAndIndices);
	}
	
	protected abstract Column<K> createPrimaryKeyColumn();
	
	protected Column<?>[] createIndexColumns() {
		return new Column<?>[0];
	}
	
	
	protected void createTableAndIndices(Connection connection) throws SQLException {
		StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
				.append(tableName)
				.append(" (");
		for (int i = 0; i < indexColumns.length + 1; i++) {
			Column<?> col = i == 0 ? primaryKeyColumn : indexColumns[i - 1];
			if (i > 0) sqlBuilder.append(", ");
			sqlBuilder
				.append(col.name())
				.append(" ")
				.append(col.sqlType());
			if (i == 0) sqlBuilder.append(" PRIMARY KEY");
		}
		sqlBuilder.append(", content VARBINARY)");
		try (PreparedStatement stmt = connection.prepareStatement(sqlBuilder.toString())) {
			stmt.execute();
		}
		for (Column<?> col : indexColumns) {
			String sqlIndex = "CREATE INDEX IF NOT EXISTS ix_%s_%s on %s (%s)".formatted(tableName, col.name(), tableName, col.name());
			try (PreparedStatement stmt = connection.prepareStatement(sqlIndex)) {
				stmt.execute();
			}			
		}
	}
	
	public V get(K key) {
		return get(primaryKeyColumn, key);
	}
	
	protected List<V> getAll() {
		List<V> result = new ArrayList<>();
		voidTransaction(c -> dbGet(c, v -> result.add(v), null, null));
		return result;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected V get(Column keyColumn, Object value) {
		MutableValue<V> result = new MutableValue<>();
		voidTransaction(c -> dbGet(c, result, keyColumn, value));
		return result.get();
	}
	
	protected void dbGet(Connection connection, Consumer<V> consumer, K key) throws SQLException {
		dbGet(connection, consumer, primaryKeyColumn, key);
	}
	
	protected <X> void dbGet(Connection connection, Consumer<V> consumer, Column<X> column, X key) throws SQLException {
		String sqlQuery = "SELECT %s, content FROM %s".formatted(primaryKeyColumn.name(), tableName);
		if (column != null && key != null) {
			sqlQuery += " WHERE %s = ?".formatted(column.name());
		}
		try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
			if (column != null && key != null) {
				column.setter().setValue(stmt, 1, key);
			}
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					byte[] bytes = rs.getBytes(2);
					if (bytes != null) {
						consumer.accept(deserializer.apply(primaryKeyColumn.getter().getValue(rs, 1), bytes));
					}
				}
			}
		}
	}


	public boolean isEmpty() {
		return transaction(c -> dbCount(c, null)) == 0;
	}

	public boolean containsKey(K key) {
		return transaction(c -> dbCount(c, key)) > 0;
	}

	protected int dbCount(Connection connection, K key) throws SQLException {
		String sqlQuery = "SELECT COUNT(*) FROM " + tableName;
		if (key != null) sqlQuery += " WHERE " + primaryKeyColumn.name() + " = ?";
		try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
			if (key != null) primaryKeyColumn.setter().setValue(stmt, 1, key);	
			try (ResultSet rs = stmt.executeQuery()) {
				rs.next();
				return rs.getInt(1);
			}
		}
	}
	
	public void put(K key, V value, Object... indexKeys) {
		if (indexKeys.length != indexColumns.length) {
			throw new IllegalArgumentException();
		}
		voidTransaction(c -> dbPut(c, key, value, indexKeys));
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void dbPut(Connection connection, K key, V value, Object... indexKeys) throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement(sqlInsert)) {
			int p = 1, n = indexKeys.length;
			primaryKeyColumn.setter().setValue(stmt, p++, key);
			for (int i = 0; i < n; i++) {
				PreparedStatementSetter setter = indexColumns[i].setter();
				setter.setValue(stmt, p++, indexKeys[i]);
			}
			byte[] bytes = serializer.apply(key, value);
			stmt.setBytes(p, bytes);
			stmt.executeUpdate();
		}
	}
	
	private String createSqlInsert() {
		StringBuilder sqlBuilder = new StringBuilder("MERGE INTO ")
				.append(tableName)
				.append(" (")
				.append(primaryKeyColumn.name())
				;
		for (Column<?> c : indexColumns) {
			sqlBuilder.append(", ").append(c.name());
		}
		sqlBuilder.append(", content) VALUES (?");
		for (int i = 0; i < indexColumns.length; i++) {
			sqlBuilder.append(", ?");
		}
		sqlBuilder.append(", ?)");
		return sqlBuilder.toString();
	}
	
	public void remove(K key) {
		voidTransaction(c -> dbRemove(c, key));
	}
	
	protected void dbRemove(Connection connection, K key) throws SQLException {
		String sqlRemove = "DELETE FROM %s".formatted(tableName);
		if (key != null) sqlRemove += " WHERE %s = ?".formatted(primaryKeyColumn.name());
		try (PreparedStatement stmt = connection.prepareStatement(sqlRemove)) {
			if (key != null) primaryKeyColumn.setter().setValue(stmt, 1, key);
			stmt.executeUpdate();
		}
	}
	
	@Override
	public void close() {

	}
	
	protected void voidTransaction(ConnectionRunnable r) {
		transaction(c -> { r.run(c); return null; });
	}
	
	protected <T> T transaction(ConnectionFunction<T> f) {
		Connection c = null;
		try {
			c = dataSource.getConnection();
			c.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException("Failed to acquire database connection", e);
		}
		try {
			T result = f.run(c);
			c.commit();
			return result;
		} catch (SQLException e) {
			rollback(c);
			throw new RuntimeException("Unexpected SQL error", e);
		} finally {
			close(c);
		}
	}
	
	private static void rollback(Connection connection) {
		try {
			connection.rollback();
		} catch (SQLException e) {
			LOGGER.error("Failed to rollback transaction", e);
		}
	}

	private static void close(Connection connection) {
		if (connection == null) return;
		try {
			connection.close();
		} catch (SQLException e) {
			LOGGER.error("Failed to close database connection", e);
		}
	}
}
