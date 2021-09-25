package org.asamk.signal.manager.storage.sessions;

import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.NoSessionException;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.protocol.CiphertextMessage;
import org.whispersystems.libsignal.state.SessionRecord;
import org.whispersystems.signalservice.api.SignalServiceSessionStore;

import io.kryptoworx.signalcli.storage.H2Map;

public class SessionStore extends H2Map<Long, SessionRecord> implements SignalServiceSessionStore {

    private final static Logger logger = LoggerFactory.getLogger(SessionStore.class);
    private final static long MAX_RECIPIENT_ID = 0xffffffffffffL;

    private final Object cachedSessions = new Object();
    private Column<Boolean> activeColumn;

    private final RecipientResolver resolver;

    public SessionStore(DataSource dataSource, final RecipientResolver resolver) {
    	super(dataSource, "sessions", (id, s) -> s.serialize(), (id, bs) -> deserialize(bs));
        this.resolver = resolver;
    }
    
    @Override
    protected Column<Long> createPrimaryKeyColumn() {
    	return new Column<>("id", "BIGINT", PreparedStatement::setLong, ResultSet::getLong);
    }
    
    @Override
    protected Column<?>[] createIndexColumns() {
    	return new Column<?>[] {
    		activeColumn = new Column<>("active", "BOOLEAN", PreparedStatement::setBoolean, ResultSet::getBoolean)
    	};
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
            List<Long> sessions = transaction(c -> dbGetSessions(c, recipientId.getId(), false));
            return sessions.stream()
            		.map(SessionStore::getDeviceId)
            		.filter(i -> i != 1)
            		.collect(Collectors.toList());
        }
    }

    private List<Long> dbGetSessions(Connection connection, long recipientId, boolean activeOnly) throws SQLException {
    	String sqlQuery = "SELECT id FROM sessions WHERE id > ?";
    	long lowerId = getKey(recipientId, 0);
    	long upperId = -1;
    	if (recipientId < MAX_RECIPIENT_ID) {
    		sqlQuery += " AND id < ?";
    		upperId = getKey(recipientId + 1, 0);
    	}
    	if (activeOnly) sqlQuery += " AND active";
    	List<Long> result = new ArrayList<>();
    	try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
    		stmt.setLong(1, lowerId);
    		if (upperId >= 0) stmt.setLong(2, upperId);;
    		try (ResultSet rs = stmt.executeQuery()) {
    			while (rs.next()) {
    				result.add(rs.getLong(1));
    			}
    		}
    	}
    	return result;
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
        	voidTransaction(c -> dbDeleteAllSessions(c, recipientId.getId()));
        }
    }
    
    private void dbDeleteAllSessions(Connection connection, long recipientId) throws SQLException {
    	String sqlQuery = "DELETE FROM sessions WHERE id > ?";
    	long lowerId = getKey(recipientId, 1);
    	long upperId = -1;
    	if (recipientId < MAX_RECIPIENT_ID) {
    		sqlQuery += " AND id < ?";
    		upperId = getKey(recipientId + 1, 0);
    	}
    	try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
    		stmt.setLong(1, lowerId);
    		if (upperId >= 0) stmt.setLong(2, upperId);;
    		stmt.executeUpdate();
    	}
    }

    @Override
    public void archiveSession(final SignalProtocolAddress address) {
        final var key = getKey(address);
        synchronized (cachedSessions) {
            archiveSessionLocked(key);
        }
    }

    @Override
    public Set<SignalProtocolAddress> getAllAddressesWithActiveSessions(final List<String> addressNames) {
        final var recipientIdToNameMap = addressNames.stream()
                .collect(Collectors.toMap(this::resolveRecipient, name -> name));
        synchronized (cachedSessions) {
            return recipientIdToNameMap.keySet()
                    .stream()
                    .flatMap(recipientId -> 
                		transaction(c -> 
            				dbGetSessions(c, recipientId.getId(), true)
            					.stream()
            					.map(k -> new SignalProtocolAddress(recipientIdToNameMap.get(recipientId), getDeviceId(k)))))
                    .collect(Collectors.toSet());
        }
    }
    
    public void archiveAllSessions() {
        synchronized (cachedSessions) {
        	voidTransaction(c -> dbArchiveSessions(c, -1));
        }
    }
    
    private void dbArchiveSessions(Connection connection, long recipientId) throws SQLException {
    	String sqlQuery = "SELECT id, content FROM sessions";
    	long lowerId = -1, upperId = -1;
    	if (recipientId >= 0) {
    		sqlQuery += " WHERE id >= ?";
    		lowerId = getKey(recipientId, 0);
    		if (recipientId < MAX_RECIPIENT_ID) {
    			sqlQuery += " AND id < ?";
    			upperId = getKey(recipientId + 1, 0);
    		}
    	}
    	try (PreparedStatement stmt = connection.prepareStatement(sqlQuery, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)) {
    		if (lowerId >= 0) stmt.setLong(1, lowerId);
    		if (upperId >= 0) stmt.setLong(2, upperId);
    		try (ResultSet rs = stmt.executeQuery()) {
    			while (rs.next()) {
    				byte[] sessionBytes = rs.getBytes(2);
    				if (sessionBytes != null) {
    					SessionRecord session = deserialize(rs.getBytes(2));
    					session.archiveCurrentState();
    					rs.updateBytes(2, session.serialize());
    					rs.updateRow();
    				}
    			}
    		}
    	}
    }

    public void archiveSessions(final RecipientId recipientId) {
        synchronized (cachedSessions) {
        	voidTransaction(c -> dbArchiveSessions(c, recipientId.getId()));
        }
    }

    public void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        synchronized (cachedSessions) {
        	voidTransaction(connection -> {
	            final var keys = dbGetSessions(connection, toBeMergedRecipientId.getId(), false);
	            final var otherHasSession = keys.size() > 0;
	            if (!otherHasSession) {
	                return;
	            }
	
	            final var hasSession = dbGetSessions(connection, recipientId.getId(), false).size() > 0;
	            if (hasSession) {
	                logger.debug("To be merged recipient had sessions, deleting.");
	                deleteAllSessions(toBeMergedRecipientId);
	            } else {
	                logger.debug("Only to be merged recipient had sessions, re-assigning to the new recipient.");
	                for (var key : keys) {
	                	var session = new MutableValue<SessionRecord>();
	                    dbGet(connection, session, key);
	                    dbRemove(connection, key);
	                    if (session.get() == null) {
	                        continue;
	                    }
	                    final var newKey = getKey(recipientId.getId(), getDeviceId(key));
	                    dbPut(connection, newKey, session.get(), isActive(session.get()));
	                }
	            }
        	});
        }
    }

    /**
     * @param identifier can be either a serialized uuid or a e164 phone number
     */
    private RecipientId resolveRecipient(String identifier) {
        return resolver.resolveRecipient(identifier);
    }

    private long getKey(final SignalProtocolAddress address) {
        final var recipientId = resolveRecipient(address.getName());
        return getKey(recipientId.getId(), address.getDeviceId());
    }
    
    private static long getKey(long recipientId, int deviceId) {
    	if (deviceId > 0xffff || recipientId > MAX_RECIPIENT_ID) {
    		throw new IllegalArgumentException();
    	}
    	return (recipientId << 16) | deviceId;
    }
    
    private static int getDeviceId(long key) {
    	return (int) (key & 0xffff);
    }
    

    private SessionRecord loadSessionLocked(long key) {
    	return get(key);
    }

    private void storeSessionLocked(long key, final SessionRecord session) {
    	put(key, session, isActive(session));
    }

    private void archiveSessionLocked(long key) {
        final var session = get(key);
        if (session == null) {
            return;
        }
        session.archiveCurrentState();
        storeSessionLocked(key, session);
    }

    private void deleteSessionLocked(long key) {
    	remove(key);
    }

    private static boolean isActive(SessionRecord record) {
        return record != null
                && record.hasSenderChain()
                && record.getSessionVersion() == CiphertextMessage.CURRENT_VERSION;
    }

    private static SessionRecord deserialize(byte[] bytes) {
    	try {
			return new SessionRecord(bytes);
		} catch (IOException e) {
			logger.warn("Failed to decode session, resetting session", e);
			return null;
		}
    }
}
