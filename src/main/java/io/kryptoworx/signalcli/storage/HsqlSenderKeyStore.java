package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.asamk.signal.manager.storage.senderKeys.ISenderKeyStore;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.groups.state.SenderKeyRecord;
import org.whispersystems.signalservice.api.push.DistributionId;

public class HsqlSenderKeyStore implements ISenderKeyStore {

    private final HsqlSenderKeyRecordStore senderKeyRecordStore;
    private final HsqlSenderKeySharedStore senderKeySharedStore;

    public HsqlSenderKeyStore(SQLConnectionFactory connectionFactory, RecipientResolver resolver) {
        this.senderKeyRecordStore = new HsqlSenderKeyRecordStore(connectionFactory, resolver);
        this.senderKeySharedStore = new HsqlSenderKeySharedStore(connectionFactory, resolver);
    }

    @Override
    public void storeSenderKey(
            final SignalProtocolAddress sender, final UUID distributionId, final SenderKeyRecord record
    ) {
        senderKeyRecordStore.storeSenderKey(sender, distributionId, record);
    }

    @Override
    public SenderKeyRecord loadSenderKey(final SignalProtocolAddress sender, final UUID distributionId) {
        return senderKeyRecordStore.loadSenderKey(sender, distributionId);
    }

    @Override
    public Set<SignalProtocolAddress> getSenderKeySharedWith(final DistributionId distributionId) {
        return senderKeySharedStore.getSenderKeySharedWith(distributionId);
    }

    @Override
    public void markSenderKeySharedWith(
            final DistributionId distributionId, final Collection<SignalProtocolAddress> addresses
    ) {
        senderKeySharedStore.markSenderKeySharedWith(distributionId, addresses);
    }

    @Override
    public void clearSenderKeySharedWith(final Collection<SignalProtocolAddress> addresses) {
        senderKeySharedStore.clearSenderKeySharedWith(addresses);
    }

    public void deleteAll() {
        senderKeySharedStore.deleteAll();
        senderKeyRecordStore.deleteAll();
    }

    public void rotateSenderKeys(RecipientId recipientId) {
        senderKeySharedStore.deleteAllFor(recipientId);
        senderKeyRecordStore.deleteAllFor(recipientId);
    }

    @Override
    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        senderKeySharedStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
        senderKeyRecordStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
    }
}
