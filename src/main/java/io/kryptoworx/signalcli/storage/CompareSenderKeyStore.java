package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.senderKeys.ISenderKeyStore;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.groups.state.SenderKeyRecord;
import org.whispersystems.signalservice.api.push.DistributionId;

public class CompareSenderKeyStore implements ISenderKeyStore {
    private final ISenderKeyStore origStore;
    private final ISenderKeyStore newStore;
    
    public CompareSenderKeyStore(ISenderKeyStore origStore, ISenderKeyStore newStore) {
        this.origStore = origStore;
        this.newStore = newStore;
    }

    public void storeSenderKey(SignalProtocolAddress sender, UUID distributionId, SenderKeyRecord record) {
        origStore.storeSenderKey(sender, distributionId, record);
        newStore.storeSenderKey(sender, distributionId, record);
    }

    public Set<SignalProtocolAddress> getSenderKeySharedWith(DistributionId distributionId) {
        var r1 = origStore.getSenderKeySharedWith(distributionId);;
        var r2 = newStore.getSenderKeySharedWith(distributionId);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public SenderKeyRecord loadSenderKey(SignalProtocolAddress sender, UUID distributionId) {
        var r1 = origStore.loadSenderKey(sender, distributionId);;
        var r2 = newStore.loadSenderKey(sender, distributionId);;
        assert(equals(r1, r2));
        return r2;
    }

    public void deleteAll() {
        origStore.deleteAll();
        newStore.deleteAll();
    }

    public void rotateSenderKeys(RecipientId recipientId) {
        origStore.rotateSenderKeys(recipientId);
        newStore.rotateSenderKeys(recipientId);
    }

    public void markSenderKeySharedWith(DistributionId distributionId, Collection<SignalProtocolAddress> addresses) {
        origStore.markSenderKeySharedWith(distributionId, addresses);
        newStore.markSenderKeySharedWith(distributionId, addresses);
    }

    public void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        origStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        newStore.mergeRecipients(recipientId, toBeMergedRecipientId);
    }

    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId)
            throws SQLException {
        origStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
        newStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
    }

    public void clearSenderKeySharedWith(Collection<SignalProtocolAddress> addresses) {
        origStore.clearSenderKeySharedWith(addresses);
        newStore.clearSenderKeySharedWith(addresses);
    }
    
    private static boolean equals(SenderKeyRecord r1, SenderKeyRecord r2) {
        if (r1 == null) return r2 == null;
        if (r2 == null) return false;
        return Arrays.equals(r1.serialize(), r2.serialize());
    }
}
