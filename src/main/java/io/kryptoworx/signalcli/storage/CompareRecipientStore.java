package io.kryptoworx.signalcli.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

import org.asamk.signal.manager.storage.recipients.Contact;
import org.asamk.signal.manager.storage.recipients.IRecipientStore;
import org.asamk.signal.manager.storage.recipients.Profile;
import org.asamk.signal.manager.storage.recipients.Recipient;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.signal.zkgroup.profiles.ProfileKey;
import org.signal.zkgroup.profiles.ProfileKeyCredential;
import org.whispersystems.libsignal.util.Pair;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.push.exceptions.UnregisteredUserException;

public class CompareRecipientStore implements IRecipientStore {
    private final IRecipientStore origStore;
    private final IRecipientStore newStore;

    public CompareRecipientStore(IRecipientStore origStore, IRecipientStore newStore) {
        this.origStore = origStore;
        this.newStore = newStore;
    }

    public RecipientAddress resolveRecipientAddress(RecipientId recipientId) {
        var r1 = origStore.resolveRecipientAddress(recipientId);;
        var r2 = newStore.resolveRecipientAddress(recipientId);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public Recipient getRecipient(RecipientId recipientId) {
        var r1 = origStore.getRecipient(recipientId);;
        var r2 = newStore.getRecipient(recipientId);;
        assert(equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipient(UUID uuid) {
        System.out.printf("DEBUG> resolveRecipient(uuid: %s)%n", uuid);
        var r1 = origStore.resolveRecipient(uuid);;
        var r2 = newStore.resolveRecipient(uuid);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipient(String identifier) {
        System.out.printf("DEBUG> resolveRecipient(identifier: %s)%n", identifier);
        var r1 = origStore.resolveRecipient(identifier);;
        var r2 = newStore.resolveRecipient(identifier);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipient(String number, Supplier<UUID> uuidSupplier) throws UnregisteredUserException {
        System.out.printf("DEBUG> resolveRecipient(number: %s)%n", number);
        var r1 = origStore.resolveRecipient(number, uuidSupplier);;
        var r2 = newStore.resolveRecipient(number, uuidSupplier);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipient(RecipientAddress address) {
        System.out.printf("DEBUG> resolveRecipient(address: %s)%n", toString(address));
        var r1 = origStore.resolveRecipient(address);;
        var r2 = newStore.resolveRecipient(address);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipient(SignalServiceAddress address) {
        System.out.printf("DEBUG> resolveRecipient(address: %s)%n", toString(address));
        var r1 = origStore.resolveRecipient(address);;
        var r2 = newStore.resolveRecipient(address);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipientTrusted(RecipientAddress address) {
        System.out.printf("DEBUG> resolveRecipientTrusted(address: %s)%n", toString(address));
        var r1 = origStore.resolveRecipientTrusted(address);;
        var r2 = newStore.resolveRecipientTrusted(address);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public RecipientId resolveRecipientTrusted(SignalServiceAddress address) {
        System.out.printf("DEBUG> resolveRecipientTrusted(address: %s)%n", toString(address));
        var r1 = origStore.resolveRecipientTrusted(address);;
        var r2 = newStore.resolveRecipientTrusted(address);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public List<RecipientId> resolveRecipientsTrusted(List<RecipientAddress> addresses) {
        var r1 = origStore.resolveRecipientsTrusted(addresses);;
        var r2 = newStore.resolveRecipientsTrusted(addresses);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public void storeContact(RecipientId recipientId, Contact contact) {
        origStore.storeContact(recipientId, contact);
        newStore.storeContact(recipientId, contact);
    }

    public Contact getContact(RecipientId recipientId) {
        var r1 = origStore.getContact(recipientId);
        var r2 = newStore.getContact(recipientId);
        assert(equals(r1, r2));
        return r2;
    }

    public List<Pair<RecipientId, Contact>> getContacts() {
        var r1 = origStore.getContacts();;
        var r2 = newStore.getContacts();;
        assert(CompareSessionStore.equals(r1, r2, CompareRecipientStore::equals));
        return r2;
    }

    public Profile getProfile(RecipientId recipientId) {
        var r1 = origStore.getProfile(recipientId);;
        var r2 = newStore.getProfile(recipientId);;
        assert(equals(r1, r2));
        return r2;
    }

    public ProfileKey getProfileKey(RecipientId recipientId) {
        var r1 = origStore.getProfileKey(recipientId);;
        var r2 = newStore.getProfileKey(recipientId);;
        assert(equals(r1, r2));
        return r2;
    }

    public ProfileKeyCredential getProfileKeyCredential(RecipientId recipientId) {
        var r1 = origStore.getProfileKeyCredential(recipientId);;
        var r2 = newStore.getProfileKeyCredential(recipientId);;
        assert(equals(r1, r2));
        return r2;
    }

    public void storeProfile(RecipientId recipientId, Profile profile) {
        origStore.storeProfile(recipientId, profile);
        newStore.storeProfile(recipientId, profile);
    }

    public void storeProfileKey(RecipientId recipientId, ProfileKey profileKey) {
        origStore.storeProfileKey(recipientId, profileKey);
        newStore.storeProfileKey(recipientId, profileKey);
    }

    public void storeProfileKeyCredential(RecipientId recipientId, ProfileKeyCredential profileKeyCredential) {
        origStore.storeProfileKeyCredential(recipientId, profileKeyCredential);
        newStore.storeProfileKeyCredential(recipientId, profileKeyCredential);
    }

    public boolean isEmpty() {
        var r1 = origStore.isEmpty();;
        var r2 = newStore.isEmpty();;
        assert(Objects.equals(r1, r2));
        return r2;
    }
    
    private static boolean equals(Profile p1, Profile p2) {
        if (p1 == null) return p2 == null;
        if (p2 == null) return false;
        return p1.getLastUpdateTimestamp() == p2.getLastUpdateTimestamp()
                && Objects.equals(p1.getGivenName(), p2.getGivenName())
                && Objects.equals(p2.getFamilyName(), p2.getFamilyName())
                && Objects.equals(p1.getAbout(), p2.getAbout())
                && Objects.equals(p1.getAboutEmoji(), p2.getAboutEmoji())
                && Objects.equals(p1.getUnidentifiedAccessMode(), p2.getUnidentifiedAccessMode())
                && Objects.equals(p1.getCapabilities(), p2.getCapabilities());
    }

    private static boolean equals(Contact c1, Contact c2) {
        if (c1 == null) return c2 == null;
        if (c2 == null) return false;
        return c1.getMessageExpirationTime() == c2.getMessageExpirationTime()
                && c1.isBlocked() == c2.isBlocked()
                && c1.isArchived() == c2.isArchived()
                && Objects.equals(c1.getName(), c2.getName())
                && Objects.equals(c2.getColor(), c2.getColor());
    }

    private static boolean equals(Recipient p1, Recipient p2) {
        if (p1 == null) return p2 == null;
        if (p2 == null) return false;
        return p1.getRecipientId().getId() == p2.getRecipientId().getId()
                && Objects.equals(p1.getAddress(), p2.getAddress())
                && Objects.equals(p2.getContact(), p2.getContact())
                && equals(p1.getProfileKey(), p2.getProfileKey())
                && equals(p1.getProfileKeyCredential(), p2.getProfileKeyCredential())
                && equals(p1.getProfile(), p2.getProfile());
    }
    
    private static boolean equals(ProfileKey k1, ProfileKey k2) {
        if (k1 == null) return k2 == null;
        if (k2 == null) return false;
        return Arrays.equals(k1.serialize(), k2.serialize());
    }

    private static boolean equals(ProfileKeyCredential c1, ProfileKeyCredential c2) {
        if (c1 == null) return c2 == null;
        if (c2 == null) return false;
        return Arrays.equals(c1.serialize(), c2.serialize());
    }
    
    private static boolean equals(Pair<RecipientId, Contact> p1, Pair<RecipientId, Contact> p2) {
        return p1.first().equals(p2) && equals(p1.second(), p2.second());
    }
    
    private static String toString(SignalServiceAddress address) {
        StringBuilder builder = new StringBuilder("SignalServiceAddress[");
        boolean addSeparator = false;
        if (address.hasValidUuid()) {
            builder.append("uuid=").append(address.getUuid());
            addSeparator = true;
        }
        if (address.getNumber().isPresent()) {
            if (addSeparator) {
                builder.append(",");
            } else {
                addSeparator = true;
            }
            builder.append("e164=").append(address.getNumber().get());
        }
        return builder.append("]").toString();
    }
    
    public static String toString(RecipientAddress address) {
        StringBuilder builder = new StringBuilder("RecipientAddress[");
        boolean addSeparator = false;
        if (address.getUuid().isPresent()) {
            builder.append("uuid=").append(address.getUuid().get());
            addSeparator = true;
        }
        if (address.getNumber().isPresent()) {
            if (addSeparator) {
                builder.append(",");
            } else {
                addSeparator = true;
            }
            builder.append("e164=").append(address.getNumber().get());
        }
        return builder.append("]").toString();
    }

}
