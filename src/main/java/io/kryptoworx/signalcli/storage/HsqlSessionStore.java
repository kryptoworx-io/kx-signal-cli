package io.kryptoworx.signalcli.storage;

import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.asamk.signal.manager.storage.sessions.ISessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.NoSessionException;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.protocol.CiphertextMessage;
import org.whispersystems.libsignal.state.SessionRecord;

public class HsqlSessionStore extends HsqlStore implements ISessionStore {

    private final static Logger logger = LoggerFactory.getLogger(HsqlSessionStore.class);

    private final Map<Key, SessionRecord> cachedSessions = new HashMap<>();
    private final RecipientResolver resolver;

    public HsqlSessionStore(SQLConnectionFactory connectionFactory, RecipientResolver resolver) {
        super(connectionFactory);
        this.resolver = resolver;
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS session
                (
                    recipient BIGINT NOT NULL
                        REFERENCES recipient(id)
                        ON DELETE CASCADE,
                    device INT NOT NULL,
                    data VARBINARY(4096) NOT NULL,
                    PRIMARY KEY (recipient, device)
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
    }

    @Override
    public SessionRecord loadSession(SignalProtocolAddress address) {
        final var key = getKey(address);

        synchronized (cachedSessions) {
            final var session = loadSessionLocked(key);
            if (session == null) {
                return new SessionRecord();
            }
            return session;
        }
    }
    
    @Override
    public List<SessionRecord> loadExistingSessions(final List<SignalProtocolAddress> addresses) throws NoSessionException {
        final var keys = addresses.stream().map(this::getKey).collect(Collectors.toList());

        synchronized (cachedSessions) {
            final var sessions = keys.stream()
                    .map(this::loadSessionLocked)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            if (sessions.size() != addresses.size()) {
                String message = "Mismatch! Asked for "
                        + addresses.size()
                        + " sessions, but only found "
                        + sessions.size()
                        + "!";
                logger.warn(message);
                throw new NoSessionException(message);
            }

            return sessions;
        }
    }
    
    @Override
    public List<Integer> getSubDeviceSessions(String name) {
        final var recipientId = resolveRecipient(name);
        synchronized (cachedSessions) {
            return getKeysLocked(recipientId).stream()
                    // get all sessions for recipient except main device session
                    .filter(key -> key.getDeviceId() != 1 && key.getRecipientId().equals(recipientId))
                    .map(Key::getDeviceId)
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void storeSession(SignalProtocolAddress address, SessionRecord session) {
        final var key = getKey(address);

        synchronized (cachedSessions) {
            storeSessionLocked(key, session);
        }
    }

    @Override
    public boolean containsSession(SignalProtocolAddress address) {
        final var key = getKey(address);

        synchronized (cachedSessions) {
            final var session = loadSessionLocked(key);
            return isActive(session);
        }
    }

    @Override
    public void deleteSession(SignalProtocolAddress address) {
        final var key = getKey(address);

        synchronized (cachedSessions) {
            deleteSessionLocked(key);
        }
    }

    @Override
    public void deleteAllSessions(String name) {
        final var recipientId = resolveRecipient(name);
        deleteAllSessions(recipientId);
    }

    public void deleteAllSessions(RecipientId recipientId) {
        synchronized (cachedSessions) {
            final var keys = getKeysLocked(recipientId);
            for (var key : keys) {
                deleteSessionLocked(key);
            }
        }
    }

    @Override
    public void archiveSession(final SignalProtocolAddress address) {
        final var key = getKey(address);
        synchronized (cachedSessions) {
            voidTransaction(c -> dbArchiveSessions(c, key.getRecipientId().getId(), key.getDeviceId()));
        }
    }

    @Override
    public Set<SignalProtocolAddress> getAllAddressesWithActiveSessions(final List<String> addressNames) {
        final var recipientIdToNameMap = addressNames.stream()
                .collect(Collectors.toMap(this::resolveRecipient, name -> name));
        synchronized (cachedSessions) {
            return recipientIdToNameMap.keySet()
                    .stream()
                    .flatMap(recipientId -> getKeysLocked(recipientId).stream())
                    .filter(key -> isActive(this.loadSessionLocked(key)))
                    .map(key -> new SignalProtocolAddress(recipientIdToNameMap.get(key.recipientId), key.getDeviceId()))
                    .collect(Collectors.toSet());
        }
    }

    public void archiveAllSessions() {
        synchronized (cachedSessions) {
            voidTransaction(c -> dbArchiveSessions(c, null, null));
        }
    }

    public void archiveSessions(final RecipientId recipientId) {
        synchronized (cachedSessions) {
            voidTransaction(c -> dbArchiveSessions(c, recipientId.getId(), null));
        }
    }

    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        synchronized (cachedSessions) {
            dbMergeRecipients(connection, recipientId, toBeMergedRecipientId);
        }
    }
    
    private void dbMergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        String sqlQuerySessions = "SELECT recipient FROM session WHERE recipient IN (?, ?) FOR UPDATE";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuerySessions, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)) {
            stmt.setLong(1, recipientId);
            stmt.setLong(2, toBeMergedRecipientId);
            try (ResultSet rs = stmt.executeQuery()) {
                int sessionCountRecipient = 0;
                int sessionCountToBeMergedRecipient = 0;
                while (rs.next()) {
                    if (rs.getLong(1) == recipientId) {
                        sessionCountRecipient++;
                    } else {
                        sessionCountToBeMergedRecipient++;
                    }
                }
                if (sessionCountToBeMergedRecipient == 0) {
                    return;
                }
                boolean deleteOtherSessions = sessionCountRecipient > 0;
                rs.beforeFirst();
                while (rs.next()) {
                    if (rs.getLong(1) == toBeMergedRecipientId) {
                        if (deleteOtherSessions) {
                            rs.deleteRow();
                        } else {
                            rs.updateLong(1, recipientId);
                            rs.updateRow();                            
                        }
                    }
                }
            }
        }
    }

    /**
     * @param identifier can be either a serialized uuid or a e164 phone number
     */
    private RecipientId resolveRecipient(String identifier) {
        return resolver.resolveRecipient(identifier);
    }

    private Key getKey(final SignalProtocolAddress address) {
        final var recipientId = resolveRecipient(address.getName());
        return new Key(recipientId, address.getDeviceId());
    }

    private List<Key> getKeysLocked(RecipientId recipientId) {
        return transaction(c -> dbLoadKeys(c, recipientId));
    }
    
    private List<Key> dbLoadKeys(Connection connection, RecipientId recipientId) throws SQLException {
        String sqlQueryKeys = "SELECT device FROM session WHERE recipient = ?";
        List<Key> keys = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryKeys)) {
            stmt.setLong(1, recipientId.getId());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    keys.add(new Key(recipientId, rs.getInt(1)));
                }
            }
        }
        return keys;
    }
    
    private SessionRecord loadSessionLocked(final Key key) {
        var session = cachedSessions.get(key);
        if (session != null) {
            return session;
        }
        session = transaction(c -> dbLoadSession(c, key));
        if (session != null) {
            cachedSessions.put(key, session);
        }
        return session;
    }
    
    private SessionRecord dbLoadSession(Connection connection, Key key) throws SQLException {
        String sqlQuery = "SELECT data FROM session WHERE recipient = ? AND device = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setLong(1, key.getRecipientId().getId());
            stmt.setInt(2, key.getDeviceId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return sessionRecord(rs.getBytes(1), false);
            }
        }
    }

    
    private SessionRecord sessionRecord(byte[] data, boolean throwOnError) {
        try {
            return new SessionRecord(data);
        } catch (IOException e) {
            logger.error("Failed to decode session record", e);
            if (throwOnError) {
                throw new AssertionError("Failed to decode session record", e);
            } else {
                return null;
            }
        }
    }

    private void storeSessionLocked(final Key key, final SessionRecord session) {
        cachedSessions.put(key, session);
        voidTransaction(c -> dbStoreSession(c, key, session));
    }
    
    private void dbStoreSession(Connection connection, Key key, SessionRecord session) throws SQLException {
        String sqlMerge = """
                MERGE INTO session t 
                USING (VALUES ?, ?) AS s(recipient, device) 
                ON (t.recipient = s.recipient AND t.device = s.device)
                WHEN MATCHED THEN UPDATE SET t.data = ?
                WHEN NOT MATCHED THEN INSERT (recipient, device, data) 
                    VALUES (s.recipient, s.device, ?) 
                """;
        byte[] sessionBytes = session.serialize();
        try (PreparedStatement stmt = connection.prepareStatement(sqlMerge)) {
            int p = 1;
            stmt.setLong(p++, key.getRecipientId().getId());
            stmt.setInt(p++, key.getDeviceId());
            stmt.setBytes(p++, sessionBytes);
            stmt.setBytes(p++, sessionBytes);
            stmt.executeUpdate();
        }
    }

    private void dbArchiveSessions(Connection connection, Long recipientId, Integer deviceId) throws SQLException {
        StringBuilder sqlQueryBuilder = new StringBuilder("SELECT recipient, device, data FROM session");
        if (recipientId != null) {
            sqlQueryBuilder.append(" WHERE recipient = ?");
        }
        if (deviceId != null) {
            sqlQueryBuilder.append(" AND device = ?");
        }
        sqlQueryBuilder.append(" FOR UPDATE");
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryBuilder.toString(), TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)) {
            int p = 1;
            if (recipientId != null) {
                stmt.setLong(p++, recipientId);
            }
            if (deviceId != null) {
                stmt.setInt(p++, deviceId);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int c = 1;
                    long r = rs.getLong(c++);
                    int d = rs.getInt(c++);
                    Key k = new Key(RecipientId.of(r), d);
                    SessionRecord s = sessionRecord(rs.getBytes(c++), false);
                    if (s != null) {
                        s.archiveCurrentState();
                        rs.updateBytes(3, s.serialize());
                        rs.updateRow();
                        cachedSessions.put(k, s);
                    }
                }
            }
        }
    }

    private void deleteSessionLocked(final Key key) {
        cachedSessions.remove(key);
        transaction(c -> dbDeleteSession(c, key));
    }
    
    private int dbDeleteSession(Connection connection, Key key) throws SQLException {
        String sqlDelete = "DELETE FROM session WHERE recipient = ? AND device = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            stmt.setLong(1, key.getRecipientId().getId());
            stmt.setInt(2, key.getDeviceId());
            return stmt.executeUpdate();
        }
    }

    private static boolean isActive(SessionRecord record) {
        return record != null
                && record.hasSenderChain()
                && record.getSessionVersion() == CiphertextMessage.CURRENT_VERSION;
    }

    private static final class Key {

        private final RecipientId recipientId;
        private final int deviceId;

        public Key(final RecipientId recipientId, final int deviceId) {
            this.recipientId = recipientId;
            this.deviceId = deviceId;
        }

        public RecipientId getRecipientId() {
            return recipientId;
        }

        public int getDeviceId() {
            return deviceId;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final var key = (Key) o;

            if (deviceId != key.deviceId) return false;
            return recipientId.equals(key.recipientId);
        }

        @Override
        public int hashCode() {
            int result = recipientId.hashCode();
            result = 31 * result + deviceId;
            return result;
        }
    }
}
