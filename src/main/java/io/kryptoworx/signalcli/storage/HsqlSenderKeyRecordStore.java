package io.kryptoworx.signalcli.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.groups.state.SenderKeyRecord;
import org.whispersystems.libsignal.groups.state.SenderKeyStore;

public class HsqlSenderKeyRecordStore extends HsqlStore implements SenderKeyStore {

    private static record Key(UUID distribution, RecipientId recipient, int device) {}
    
    private final Object lock = new Object();

    private final RecipientResolver resolver;

    public HsqlSenderKeyRecordStore(SQLConnectionFactory connectionFactory, RecipientResolver resolver) {
        super(connectionFactory);
        this.resolver = resolver;
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS sender_key
                (
                    recipient BIGINT NOT NULL REFERENCES recipient(id) ON DELETE CASCADE,
                    device INT NOT NULL,
                    distribution UUID NOT NULL,
                    data VARBINARY(1024) NOT NULL,
                    PRIMARY KEY (recipient, device, distribution)
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
    }

    @Override
    public SenderKeyRecord loadSenderKey(final SignalProtocolAddress address, final UUID distributionId) {
        var key = getKey(address, distributionId);
        synchronized (lock) {
            return transaction(c -> dbLoadSenderKey(c, key));
        }
    }
    
    private SenderKeyRecord dbLoadSenderKey(Connection connection, Key key) throws SQLException {
        String sqlQuery = "SELECT data FROM sender_key WHERE recipient = ? AND device = ? AND distribution = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setLong(1, key.recipient().getId());
            stmt.setInt(2, key.device());
            stmt.setObject(3, key.distribution());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return keyRecord(rs.getBytes(1));
            }
        }
    }

    @Override
    public void storeSenderKey(SignalProtocolAddress address, UUID distributionId, SenderKeyRecord record) {
        final var key = getKey(address, distributionId);
        synchronized (lock) {
            voidTransaction(c -> dbStoreSenderKey(c, key, record));
        }
    }
    
    private void dbStoreSenderKey(Connection connection, Key key, SenderKeyRecord record) throws SQLException {
        String sqlMerge = """
                MERGE INTO sender_key t
                USING (VALUES ?, ?, ?, CAST(? AS VARBINARY(1024))) AS s(rcpt, dev, dist, data)
                ON t.recipient = s.rcpt AND t.device = s.dev AND t.distribution = s.dist
                WHEN MATCHED THEN UPDATE SET t.data = s.data
                WHEN NOT MATCHED THEN INSERT (recipient, device, distribution, data)
                    VALUES (s.rcpt, s.dev, s.dist, s.data)
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlMerge)) {
            int p = 1;
            stmt.setLong(p++, key.recipient().getId());
            stmt.setInt(p++, key.device());
            stmt.setObject(p++, key.distribution());
            stmt.setBytes(p++, record.serialize());
            stmt.executeUpdate();
        }
    }

    public void deleteAll() {
        synchronized (lock) {
            voidTransaction(c -> dbDeleteSenderKeys(c, null));
        }
    }

    public void deleteAllFor(RecipientId recipientId) {
        synchronized (lock) {
            voidTransaction(c -> dbDeleteSenderKeys(c, recipientId));
        }
    }
    
    private void dbDeleteSenderKeys(Connection connection, RecipientId recipient) throws SQLException {
        String sqlDelete = "DELETE FROM sender_key";
        if (recipient != null) sqlDelete += " WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            if (recipient != null) stmt.setLong(1, recipient.getId());
            stmt.executeUpdate();
        }
    }

    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        String sqlUpdate = "UPDATE sender_key SET recipient = ? WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlUpdate)) {
            stmt.setLong(1, recipientId);
            stmt.setLong(2, toBeMergedRecipientId);
            stmt.executeUpdate();
        }
    }

    /**
     * @param identifier can be either a serialized uuid or a e164 phone number
     */
    private RecipientId resolveRecipient(String identifier) {
        return resolver.resolveRecipient(identifier);
    }

    private Key getKey(final SignalProtocolAddress address, final UUID distributionId) {
        final var recipientId = resolveRecipient(address.getName());
        return new Key(distributionId, recipientId, address.getDeviceId());
    }

    private static SenderKeyRecord keyRecord(byte[] data) {
        try {
            return new SenderKeyRecord(data);
        } catch (IOException e) {
            throw new AssertionError("Failed to decode SenderKeyRecord");
        }
    }
}
