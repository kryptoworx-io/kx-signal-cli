package org.asamk.signal.manager.storage.recipients;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.asamk.signal.manager.storage.contacts.ContactsStore;
import org.asamk.signal.manager.storage.profiles.ProfileStore;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.push.exceptions.UnregisteredUserException;

public interface IRecipientStore extends RecipientResolver, ContactsStore, ProfileStore {

    RecipientAddress resolveRecipientAddress(RecipientId recipientId);

    Recipient getRecipient(RecipientId recipientId);

    RecipientId resolveRecipient(String number, Supplier<UUID> uuidSupplier) throws UnregisteredUserException;

    RecipientId resolveRecipientTrusted(RecipientAddress address);

    RecipientId resolveRecipientTrusted(SignalServiceAddress address);

    List<RecipientId> resolveRecipientsTrusted(List<RecipientAddress> addresses);

    boolean isEmpty();

}