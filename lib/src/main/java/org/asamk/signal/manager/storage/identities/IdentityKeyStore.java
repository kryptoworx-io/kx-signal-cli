package org.asamk.signal.manager.storage.identities;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.IdentityKey;
import org.whispersystems.libsignal.IdentityKeyPair;
import org.whispersystems.libsignal.SignalProtocolAddress;

import io.kryptoworx.signalcli.storage.H2IdentityMap;

public class IdentityKeyStore implements org.whispersystems.libsignal.state.IdentityKeyStore, AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(IdentityKeyStore.class);

    private final H2IdentityMap identityMap;

    private final RecipientResolver resolver;
    private final IdentityKeyPair identityKeyPair;
    private final int localRegistrationId;
    private final TrustNewIdentity trustNewIdentity;

    public IdentityKeyStore(
            final DataSource dataSource,
            final RecipientResolver resolver,
            final IdentityKeyPair identityKeyPair,
            final int localRegistrationId,
            final TrustNewIdentity trustNewIdentity,
            final byte[] masterKey
    ) throws IOException {
        this.resolver = resolver;
        this.identityKeyPair = identityKeyPair;
        this.localRegistrationId = localRegistrationId;
        this.trustNewIdentity = trustNewIdentity;
        this.identityMap = new H2IdentityMap(dataSource);
    }

    @Override
    public IdentityKeyPair getIdentityKeyPair() {
        return identityKeyPair;
    }

    @Override
    public int getLocalRegistrationId() {
        return localRegistrationId;
    }

    @Override
    public boolean saveIdentity(SignalProtocolAddress address, IdentityKey identityKey) {
        final var recipientId = resolveRecipient(address.getName());

        return saveIdentity(recipientId, identityKey, new Date());
    }

    public boolean saveIdentity(final RecipientId recipientId, final IdentityKey identityKey, Date added) {
        synchronized (identityMap) {
            final var identityInfo = loadIdentityLocked(recipientId);
            if (identityInfo != null && identityInfo.getIdentityKey().equals(identityKey)) {
                // Identity already exists, not updating the trust level
                return false;
            }

            final var trustLevel = trustNewIdentity == TrustNewIdentity.ALWAYS || (
                    trustNewIdentity == TrustNewIdentity.ON_FIRST_USE && identityInfo == null
            ) ? TrustLevel.TRUSTED_UNVERIFIED : TrustLevel.UNTRUSTED;
            logger.debug("Storing new identity for recipient {} with trust {}", recipientId, trustLevel);
            final var newIdentityInfo = new IdentityInfo(recipientId, identityKey, trustLevel, added);
            storeIdentityLocked(recipientId, newIdentityInfo);
            return true;
        }
    }

    public boolean setIdentityTrustLevel(
            RecipientId recipientId, IdentityKey identityKey, TrustLevel trustLevel
    ) {
        synchronized (identityMap) {
            final var identityInfo = loadIdentityLocked(recipientId);
            if (identityInfo == null || !identityInfo.getIdentityKey().equals(identityKey)) {
                // Identity not found, not updating the trust level
                return false;
            }

            final var newIdentityInfo = new IdentityInfo(recipientId,
                    identityKey,
                    trustLevel,
                    identityInfo.getDateAdded());
            storeIdentityLocked(recipientId, newIdentityInfo);
            return true;
        }
    }

    @Override
    public boolean isTrustedIdentity(SignalProtocolAddress address, IdentityKey identityKey, Direction direction) {
        if (trustNewIdentity == TrustNewIdentity.ALWAYS) {
            return true;
        }

        var recipientId = resolveRecipient(address.getName());

        synchronized (identityMap) {
            final var identityInfo = loadIdentityLocked(recipientId);
            if (identityInfo == null) {
                // Identity not found
                return trustNewIdentity == TrustNewIdentity.ON_FIRST_USE;
            }

            // TODO implement possibility for different handling of incoming/outgoing trust decisions
            if (!identityInfo.getIdentityKey().equals(identityKey)) {
                // Identity found, but different
                return false;
            }

            return identityInfo.isTrusted();
        }
    }

    @Override
    public IdentityKey getIdentity(SignalProtocolAddress address) {
        var recipientId = resolveRecipient(address.getName());

        synchronized (identityMap) {
            var identity = loadIdentityLocked(recipientId);
            return identity == null ? null : identity.getIdentityKey();
        }
    }

    public IdentityInfo getIdentity(RecipientId recipientId) {
        synchronized (identityMap) {
            return loadIdentityLocked(recipientId);
        }
    }

    public List<IdentityInfo> getIdentities() {
    	synchronized (identityMap) {
    		return identityMap.getIdentities();
    	}
    }

    public void mergeRecipients(final RecipientId recipientId, final RecipientId toBeMergedRecipientId) {
        synchronized (identityMap) {
            deleteIdentityLocked(toBeMergedRecipientId);
        }
    }

    /**
     * @param identifier can be either a serialized uuid or a e164 phone number
     */
    private RecipientId resolveRecipient(String identifier) {
        return resolver.resolveRecipient(identifier);
    }

    private IdentityInfo loadIdentityLocked(final RecipientId recipientId) {
    	return identityMap.get(recipientId.getId());
    }

    private void storeIdentityLocked(final RecipientId recipientId, final IdentityInfo identityInfo) {
        identityMap.put(recipientId.getId(), identityInfo);
    }

    private void deleteIdentityLocked(final RecipientId recipientId) {
        identityMap.remove(recipientId.getId());
    }

	@Override
	public void close() {
		identityMap.close();
	}

}
