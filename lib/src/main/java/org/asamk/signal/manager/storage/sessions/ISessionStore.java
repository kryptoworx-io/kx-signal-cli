package org.asamk.signal.manager.storage.sessions;

import java.sql.Connection;
import java.sql.SQLException;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.signalservice.api.SignalServiceSessionStore;

public interface ISessionStore extends SignalServiceSessionStore {

    void deleteAllSessions(RecipientId recipientId);

    void archiveAllSessions();

    void archiveSessions(RecipientId recipientId);

    default void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        
    }
    
    default void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        
    }

}