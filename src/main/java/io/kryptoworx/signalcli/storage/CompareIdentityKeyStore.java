package io.kryptoworx.signalcli.storage;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.identities.IIdentityKeyStore;
import org.asamk.signal.manager.storage.identities.IdentityInfo;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.libsignal.IdentityKey;
import org.whispersystems.libsignal.IdentityKeyPair;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.state.IdentityKeyStore.Direction;

public class CompareIdentityKeyStore implements IIdentityKeyStore {

    private final IIdentityKeyStore origStore;
    private final IIdentityKeyStore newStore;

    public CompareIdentityKeyStore(IIdentityKeyStore origStore, IIdentityKeyStore newStore) {
        this.origStore = origStore;
        this.newStore = newStore;
    }

    public IdentityKeyPair getIdentityKeyPair() {
        var r1 = origStore.getIdentityKeyPair();
        var r2 = newStore.getIdentityKeyPair();
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public int getLocalRegistrationId() {
        var r1 = origStore.getLocalRegistrationId();
        var r2 = newStore.getLocalRegistrationId();
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public boolean saveIdentity(SignalProtocolAddress address, IdentityKey identityKey) {
        var r1 = origStore.saveIdentity(address, identityKey);
        var r2 = newStore.saveIdentity(address, identityKey);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public boolean saveIdentity(RecipientId recipientId, IdentityKey identityKey, Date added) {
        var r1 = origStore.saveIdentity(recipientId, identityKey, added);
        var r2 = newStore.saveIdentity(recipientId, identityKey, added);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public boolean setIdentityTrustLevel(RecipientId recipientId, IdentityKey identityKey, TrustLevel trustLevel) {
        var r1 = origStore.setIdentityTrustLevel(recipientId, identityKey, trustLevel);
        var r2 = newStore.setIdentityTrustLevel(recipientId, identityKey, trustLevel);
        assert (Objects.equals(r1, r2));
        return r2;
    }

    public boolean isTrustedIdentity(SignalProtocolAddress address, IdentityKey identityKey, Direction direction) {
        var r1 = origStore.isTrustedIdentity(address, identityKey, direction);;
        var r2 = newStore.isTrustedIdentity(address, identityKey, direction);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public IdentityKey getIdentity(SignalProtocolAddress address) {
        var r1 = origStore.getIdentity(address);;
        var r2 = newStore.getIdentity(address);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public IdentityInfo getIdentity(RecipientId recipientId) {
        var r1 = origStore.getIdentity(recipientId);;
        var r2 = newStore.getIdentity(recipientId);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public List<IdentityInfo> getIdentities() {
        var r1 = origStore.getIdentities();;
        var r2 = newStore.getIdentities();;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        origStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        newStore.mergeRecipients(recipientId, toBeMergedRecipientId);
    }
}
