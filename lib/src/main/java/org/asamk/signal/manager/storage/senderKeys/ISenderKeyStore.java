package org.asamk.signal.manager.storage.senderKeys;

import java.sql.Connection;
import java.sql.SQLException;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.signalservice.api.SignalServiceSenderKeyStore;

public interface ISenderKeyStore extends SignalServiceSenderKeyStore {

    void deleteAll();

    void rotateSenderKeys(RecipientId recipientId);

    default void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) { }
    default void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException { }

}