package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.sessions.ISessionStore;
import org.whispersystems.libsignal.NoSessionException;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.state.SessionRecord;

public class CompareSessionStore implements ISessionStore {

    private final ISessionStore origSessionStore;
    private final ISessionStore newSessionStore;
    
    public CompareSessionStore(ISessionStore origSessionStore, ISessionStore newSessionStore) {
        this.origSessionStore = origSessionStore;
        this.newSessionStore = newSessionStore;
    }

    @Override
    public SessionRecord loadSession(SignalProtocolAddress address) {
        SessionRecord r1 = origSessionStore.loadSession(address);
        SessionRecord r2 = newSessionStore.loadSession(address);
        assert (equals(r1, r2));
        return r2;
    }

    public List<SessionRecord> loadExistingSessions(List<SignalProtocolAddress> addresses) throws NoSessionException {
        List<SessionRecord> r1 = origSessionStore.loadExistingSessions(addresses);
        List<SessionRecord> r2 = newSessionStore.loadExistingSessions(addresses);
        assert (equals(r1, r2, CompareSessionStore::equals));
        return r2;
    }

    public List<Integer> getSubDeviceSessions(String name) {
        var r1 = origSessionStore.getSubDeviceSessions(name);
        var r2 = newSessionStore.getSubDeviceSessions(name);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public void storeSession(SignalProtocolAddress address, SessionRecord session) {
        origSessionStore.storeSession(address, session);
        newSessionStore.storeSession(address, session);
    }

    public boolean containsSession(SignalProtocolAddress address) {
        var r1 = origSessionStore.containsSession(address);
        var r2 = newSessionStore.containsSession(address);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public void deleteSession(SignalProtocolAddress address) {
        origSessionStore.deleteSession(address);
        newSessionStore.deleteSession(address);
    }

    public void deleteAllSessions(String name) {
        origSessionStore.deleteAllSessions(name);
        newSessionStore.deleteAllSessions(name);
    }

    public void deleteAllSessions(RecipientId recipientId) {
        origSessionStore.deleteAllSessions(recipientId);
        newSessionStore.deleteAllSessions(recipientId);
    }

    public void archiveSession(SignalProtocolAddress address) {
        origSessionStore.archiveSession(address);
        newSessionStore.archiveSession(address);
    }

    public Set<SignalProtocolAddress> getAllAddressesWithActiveSessions(List<String> addressNames) {
        var r1 = origSessionStore.getAllAddressesWithActiveSessions(addressNames);
        var r2 = newSessionStore.getAllAddressesWithActiveSessions(addressNames);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public void archiveAllSessions() {
        origSessionStore.archiveAllSessions();
        newSessionStore.archiveAllSessions();
    }

    public void archiveSessions(RecipientId recipientId) {
        origSessionStore.archiveSessions(recipientId);
        newSessionStore.archiveSessions(recipientId);
    }

    public void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        origSessionStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        newSessionStore.mergeRecipients(recipientId, toBeMergedRecipientId);
    }
    
    @Override
    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId)
            throws SQLException {
        origSessionStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
        newSessionStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
    }
    
    static <T> boolean equals(Collection<T> c1, Collection<T> c2, BiFunction<T, T, Boolean> equals) {
        if (c1 == null) {
            return c2 == null;
        } else if (c2 != null) {
            if (c1.size() != c2.size()) return false;
            Iterator<T> i1 = c1.iterator();
            Iterator<T> i2 = c2.iterator();
            while (i1.hasNext()) {
                if (!equals.apply(i1.next(), i2.next())) return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    private static boolean equals(SessionRecord s1, SessionRecord s2) {
        return Arrays.equals(s1.serialize(), s2.serialize());
    }
}
