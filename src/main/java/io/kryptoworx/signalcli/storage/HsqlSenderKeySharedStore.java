package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.signalservice.api.push.DistributionId;

public class HsqlSenderKeySharedStore extends HsqlStore {
    
    private static final record SenderKeySharedEntry(RecipientId recipientId, int deviceId) { }
    
    private final Object lock = new Object();

    private final RecipientResolver resolver;

    public HsqlSenderKeySharedStore(SQLConnectionFactory connectionFactory, RecipientResolver resolver) {
        super(connectionFactory);
        this.resolver = resolver;
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS sender_key_shared
                (
                    distribution UUID NOT NULL,
                    recipient BIGINT NOT NULL REFERENCES recipient(ID) ON DELETE CASCADE,
                    device INT NOT NULL,
                    PRIMARY KEY (distribution, recipient, device)
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
    }

    public Set<SignalProtocolAddress> getSenderKeySharedWith(final DistributionId distributionId) {
        synchronized (lock) {
            return transaction(c -> dbLoadSenderKeySharedWith(c, distributionId.asUuid()));
        }
    }
    
    private Set<SignalProtocolAddress> dbLoadSenderKeySharedWith(Connection connection, UUID distributionId) throws SQLException {
        String sqlQuery = """
                SELECT r.guid, r.e164, s.device 
                FROM sender_key_shared s
                JOIN recipient r on s.recipient = r.id
                WHERE s.distribution = ?
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setObject(1, distributionId);
            Set<SignalProtocolAddress> result = new HashSet<>();
            try (ResultSet rs = stmt.executeQuery()) {
                int c = 1;
                Object uuid = rs.getObject(c++);
                String e164 = rs.getString(c++);
                int device = rs.getInt(c++);
                result.add(new SignalProtocolAddress(uuid != null ? uuid.toString() : e164, device));
            }
            return result;
        }
    }

    public void markSenderKeySharedWith(DistributionId distributionId, Collection<SignalProtocolAddress> addresses) {
        var newEntries = addresses.stream()
                .map(a -> new SenderKeySharedEntry(resolveRecipient(a.getName()), a.getDeviceId()))
                .toArray(SenderKeySharedEntry[]::new);
        synchronized (lock) {
            voidTransaction(c -> dbMarkSenderKeySharedWith(c, distributionId.asUuid(), newEntries));
        }
    }
    
    private void dbMarkSenderKeySharedWith(Connection connection, UUID distributionId, 
            SenderKeySharedEntry[] newEntries) throws SQLException {
        String sqlMerge = """
                MERGE INTO sender_key_shared t
                USING (VALUES CAST(? AS UUID), ?, ?) AS s(dist, rcpt, dev)
                ON t.distribution = s.dist AND t.recipient = s.rcpt AND r.device = s.dev
                WHEN NOT MATCHED THEN INSERT (distribution, recipient, device)
                    VALUES (s.dist, s.rcpt, t.dev)
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlMerge)) {
            for (var e : newEntries) {
                int p = 1;
                stmt.setObject(p++, distributionId);
                stmt.setLong(p++, e.recipientId().getId());
                stmt.setInt(p++, e.deviceId());
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    public void clearSenderKeySharedWith(Collection<SignalProtocolAddress> addresses) {
        var entriesToDelete = addresses.stream()
                .map(a -> new SenderKeySharedEntry(resolveRecipient(a.getName()), a.getDeviceId()))
                .toArray(SenderKeySharedEntry[]::new);
        synchronized (lock) {
            voidTransaction(c -> dbDelete(c, entriesToDelete));
        }
    }
    
    private void dbDelete(Connection connection, SenderKeySharedEntry[] entriesToDelete) throws SQLException {
        String sqlDelete = "DELETE FROM sender_key_shared WHERE recipient = ? AND device = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            for (var e : entriesToDelete) {
                stmt.setLong(1, e.recipientId().getId());
                stmt.setInt(2, e.deviceId());
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }

    public void deleteAll() {
        synchronized (lock) {
            voidTransaction(c -> dbDeleteForRecipient(c, null));
        }
    }

    public void deleteAllFor(final RecipientId recipientId) {
        synchronized (lock) {
            voidTransaction(c -> dbDeleteForRecipient(c, recipientId));
        }
    }
    
    private void dbDeleteForRecipient(Connection connection, RecipientId recipient) throws SQLException {
        String sqlDelete = "DELETE FROM sender_key_shared";
        if (recipient != null) sqlDelete += " WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            if (recipient != null) stmt.setLong(1, recipient.getId());
            stmt.executeUpdate();
        }
    }

    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        synchronized (lock) {
            dbMergeRecipients(connection, recipientId, toBeMergedRecipientId);
        }
    }

    private void dbMergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        String sqlUpdate = "UPDATE sender_key_shared SET recipient = ? WHERE recipient = ?";
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
}
