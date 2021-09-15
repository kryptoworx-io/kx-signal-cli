package org.asamk.signal.manager.storage.identities;

import java.util.Date;
import java.util.List;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.libsignal.IdentityKey;

public interface IIdentityKeyStore extends org.whispersystems.libsignal.state.IdentityKeyStore {

    boolean saveIdentity(RecipientId recipientId, IdentityKey identityKey, Date added);

    boolean setIdentityTrustLevel(RecipientId recipientId, IdentityKey identityKey, TrustLevel trustLevel);

    IdentityInfo getIdentity(RecipientId recipientId);

    List<IdentityInfo> getIdentities();

    default void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        
    }

}