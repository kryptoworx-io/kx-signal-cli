package org.asamk.signal.manager.storage.recipients;

import java.sql.Connection;
import java.sql.SQLException;

public interface RecipientMergeHandler {
    void beforeMergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException;
    void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId);
}