package io.kryptoworx.signalcli.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.asamk.signal.manager.storage.prekeys.ISignedPreKeyStore;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;
import org.whispersystems.libsignal.state.SignedPreKeyStore;

public class HsqlSignedPreKeyStore extends HsqlStore implements SignedPreKeyStore, ISignedPreKeyStore {


    public HsqlSignedPreKeyStore(SQLConnectionFactory connectionFactory) {
        super(connectionFactory);
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String createSequence = """
                CREATE SEQUENCE IF NOT EXISTS signed_prekey_seq AS INT 
                START WITH 0 
                INCREMENT BY 1 
                MAXVALUE 16777215 
                CYCLE
                """;
        try (PreparedStatement stmt = connection.prepareStatement(createSequence)) {
            stmt.execute();
        }
        String createTable = """
                CREATE TABLE IF NOT EXISTS signed_prekey
                (
                    id INT NOT NULL PRIMARY KEY,
                    key VARBINARY(200) NOT NULL
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(createTable)) {
            stmt.execute();
        }
        
    }

    @Override
    public SignedPreKeyRecord loadSignedPreKey(int signedPreKeyId) throws InvalidKeyIdException {
        String sqlQuery = "SELECT key FROM signed_prekey WHERE id = ?";
        return transaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlQuery)) {
                stmt.setInt(1, signedPreKeyId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) new InvalidKeyIdException("Can't find signed pre-key with id " + signedPreKeyId);
                    return keyRecord(rs.getBytes(1));
                }
            }
        });
    }

    @Override
    public List<SignedPreKeyRecord> loadSignedPreKeys() {
        String sqlQuery = "SELECT key FROM signed_prekey";
        return transaction(c -> {
            List<SignedPreKeyRecord> keys = new ArrayList<>();
            try (PreparedStatement stmt = c.prepareStatement(sqlQuery)) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        keys.add(keyRecord(rs.getBytes(1)));
                    }
                }
            }
            return keys;
        });
    }

    @Override
    public void storeSignedPreKey(int signedPreKeyId, SignedPreKeyRecord record) {
        String sqlInsert = "INSERT INTO signed_prekey (id, key) VALUES (?, ?)";
        voidTransaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlInsert)) {
                stmt.setInt(1, signedPreKeyId);
                stmt.setBytes(2, record.serialize());
                stmt.executeUpdate();
            }
        });    }

    @Override
    public boolean containsSignedPreKey(int signedPreKeyId) {
        String sqlQuery = "SELECT COUNT(*) FROM signed_prekey WHERE id = ?";
        return transaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlQuery)) {
                stmt.setInt(1, signedPreKeyId);
                try (ResultSet rs = stmt.executeQuery()) {
                    return rs.next() && rs.getInt(1) > 0;
                }
            }
        });
    }

    @Override
    public void removeSignedPreKey(int signedPreKeyId) {
        String sqlDelete = "DELETE FROM signed_prekey WHERE id = ?";
        voidTransaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlDelete)) {
                stmt.setInt(1, signedPreKeyId);
                stmt.executeUpdate();
            }
        });
    }

    public void removeAllSignedPreKeys() {
        voidTransaction(this::dbRemoveAllSignedPreKeys);
    }

    private void dbRemoveAllSignedPreKeys(Connection connection) throws SQLException {
        String sqlDelete = "DELETE FROM signed_prekey";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            stmt.executeUpdate();
        }
        String sqlResetSeq = "ALTER SEQUENCE signed_prekey_seq RESTART WITH 0";
        try (PreparedStatement stmt = connection.prepareStatement(sqlResetSeq)) {
            stmt.executeUpdate();
        }        
    }
    
    public int getNextSignedPreKeyId() {
        return transaction(c -> dbGetNextSignedPreKeyId(c));
    }

    private int dbGetNextSignedPreKeyId(Connection connection) throws SQLException {
        String sqlQuery = "SELECT t.next_id FROM (VALUES NEXT VALUE FOR signed_prekey_seq) AS t(next_id)";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery);
                ResultSet rs = stmt.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static SignedPreKeyRecord keyRecord(byte[] bytes) {
        try {
            return new SignedPreKeyRecord(bytes);
        } catch (IOException e) {
            throw new AssertionError("Failed to decode signed pre-key");
        }
    }
}
