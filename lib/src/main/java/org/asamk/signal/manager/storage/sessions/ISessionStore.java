package org.asamk.signal.manager.storage.sessions;

import java.io.IOException;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.signalservice.api.SignalServiceSessionStore;

public interface ISessionStore extends SignalServiceSessionStore {

	void deleteAllSessions(RecipientId recipientId);

	void archiveAllSessions();

	void archiveSessions(RecipientId recipientId);

	void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId);

	default void close() throws IOException { }
	
}