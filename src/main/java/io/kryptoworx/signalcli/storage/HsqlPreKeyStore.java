package io.kryptoworx.signalcli.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.asamk.signal.manager.storage.prekeys.IPreKeyStore;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.PreKeyRecord;
import org.whispersystems.libsignal.state.PreKeyStore;

public class HsqlPreKeyStore extends HsqlStore implements PreKeyStore, IPreKeyStore {
    
    public HsqlPreKeyStore(SQLConnectionFactory connectionFactory) {
        super(connectionFactory);
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String createSequence = """
                CREATE SEQUENCE IF NOT EXISTS prekey_seq AS INT 
                START WITH 0 
                INCREMENT BY 100 
                MAXVALUE 16777215 
                CYCLE
                """;
        try (PreparedStatement stmt = connection.prepareStatement(createSequence)) {
            stmt.execute();
        }
        String createTable = """
                CREATE TABLE IF NOT EXISTS prekey
                (
                    id INT NOT NULL PRIMARY KEY,
                    key VARBINARY(100) NOT NULL
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(createTable)) {
            stmt.execute();
        }
    }

    @Override
    public PreKeyRecord loadPreKey(int preKeyId) throws InvalidKeyIdException {
        String sqlQuery = "SELECT key FROM prekey WHERE id = ?";
        return transaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlQuery)) {
                stmt.setInt(1, preKeyId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) new InvalidKeyIdException("Can't find pre-key with id " + preKeyId);
                    return keyRecord(rs.getBytes(1));
                }
            }
        });
    }

    @Override
    public void storePreKey(int preKeyId, PreKeyRecord record) {
        String sqlInsert = "INSERT INTO prekey (id, key) VALUES (?, ?)";
        voidTransaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlInsert)) {
                stmt.setInt(1, preKeyId);
                stmt.setBytes(2, record.serialize());
                stmt.executeUpdate();
            }
        });
    }

    @Override
    public boolean containsPreKey(int preKeyId) {
        String sqlQuery = "SELECT COUNT(*) FROM prekey WHERE id = ?";
        return transaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlQuery)) {
                stmt.setInt(1, preKeyId);
                try (ResultSet rs = stmt.executeQuery()) {
                    return rs.next() && rs.getInt(1) > 0;
                }
            }
        });
    }

    @Override
    public void removePreKey(int preKeyId) {
        String sqlDelete = "DELETE FROM prekey WHERE id = ?";
        voidTransaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlDelete)) {
                stmt.setInt(1, preKeyId);
                stmt.executeUpdate();
            }
        });
    }
    
    public void removeAllPreKeys() {
        voidTransaction(this::dbRemoveAllPreKeys);
    }
    
    private void dbRemoveAllPreKeys(Connection connection) throws SQLException {
        String sqlDelete = "DELETE FROM prekey";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            stmt.executeUpdate();
        }
        String sqlResetSeq = "ALTER SEQUENCE prekey_seq RESTART WITH 0";
        try (PreparedStatement stmt = connection.prepareStatement(sqlResetSeq)) {
            stmt.executeUpdate();
        }        
    }

    public int getPreKeyIdOffset() {
        return transaction(c -> dbGetPreKeyIdOffet(c));
    }
    
    private int dbGetPreKeyIdOffet(Connection connection) throws SQLException {
        String sqlQuery = "SELECT t.next_id FROM (VALUES NEXT VALUE FOR prekey_seq) AS t(next_id)";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery);
                ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static PreKeyRecord keyRecord(byte[] bytes) {
        try {
            return new PreKeyRecord(bytes);
        } catch (IOException e) {
            throw new AssertionError("Failed to decode pre-key");
        }
    }
}
