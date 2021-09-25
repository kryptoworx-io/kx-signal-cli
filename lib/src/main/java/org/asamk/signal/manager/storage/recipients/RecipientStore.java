package org.asamk.signal.manager.storage.recipients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.asamk.signal.manager.storage.Utils;
import org.asamk.signal.manager.storage.contacts.ContactsStore;
import org.asamk.signal.manager.storage.profiles.ProfileStore;
import org.signal.zkgroup.profiles.ProfileKey;
import org.signal.zkgroup.profiles.ProfileKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.util.Pair;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.push.exceptions.UnregisteredUserException;

import io.kryptoworx.signalcli.storage.H2RecipientMap;

public class RecipientStore implements RecipientResolver, ContactsStore, ProfileStore, AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(RecipientStore.class);

    private final RecipientMergeHandler recipientMergeHandler;

    private final H2RecipientMap recipients;
    private final Map<RecipientId, RecipientId> recipientsMerged = new HashMap<>();

    public static RecipientStore load(DataSource dataSource, RecipientMergeHandler recipientMergeHandler) throws IOException {
    	return new RecipientStore(recipientMergeHandler, new H2RecipientMap(dataSource));
    }

    private RecipientStore(
            final RecipientMergeHandler recipientMergeHandler,
            final H2RecipientMap recipients
    ) {
        this.recipientMergeHandler = recipientMergeHandler;
        this.recipients = recipients;
    }

    public RecipientAddress resolveRecipientAddress(RecipientId recipientId) {
        synchronized (recipients) {
            return getRecipient(recipientId).getAddress();
        }
    }

    public Recipient getRecipient(RecipientId recipientId) {
        synchronized (recipients) {
            while (recipientsMerged.containsKey(recipientId)) {
                recipientId = recipientsMerged.get(recipientId);
            }
            return recipients.get(recipientId);
        }
    }
    
    @Override
    public RecipientId resolveRecipient(UUID uuid) {
        return resolveRecipient(new RecipientAddress(uuid), false);
    }

    @Override
    public RecipientId resolveRecipient(final String identifier) {
        return resolveRecipient(Utils.getRecipientAddressFromIdentifier(identifier), false);
    }

    public RecipientId resolveRecipient(
            final String number, Supplier<UUID> uuidSupplier
    ) throws UnregisteredUserException {
        final Optional<Recipient> byNumber;
        synchronized (recipients) {
            byNumber = findByNumberLocked(number);
        }
        if (byNumber.isEmpty() || byNumber.get().getAddress().getUuid().isEmpty()) {
            final var uuid = uuidSupplier.get();
            if (uuid == null) {
                throw new UnregisteredUserException(number, null);
            }

            return resolveRecipient(new RecipientAddress(uuid, number), false);
        }
        return byNumber.get().getRecipientId();
    }

    public RecipientId resolveRecipient(RecipientAddress address) {
        return resolveRecipient(address, false);
    }

    @Override
    public RecipientId resolveRecipient(final SignalServiceAddress address) {
        return resolveRecipient(new RecipientAddress(address), false);
    }

    public RecipientId resolveRecipientTrusted(RecipientAddress address) {
        return resolveRecipient(address, true);
    }

    public RecipientId resolveRecipientTrusted(SignalServiceAddress address) {
        return resolveRecipient(new RecipientAddress(address), true);
    }

    public List<RecipientId> resolveRecipientsTrusted(List<RecipientAddress> addresses) {
        final List<RecipientId> recipientIds;
        final List<Pair<RecipientId, RecipientId>> toBeMerged = new ArrayList<>();
        synchronized (recipients) {
            recipientIds = addresses.stream().map(address -> {
                final var pair = resolveRecipientLocked(address, true);
                if (pair.second().isPresent()) {
                    toBeMerged.add(new Pair<>(pair.first(), pair.second().get()));
                }
                return pair.first();
            }).collect(Collectors.toList());
        }
        for (var pair : toBeMerged) {
            recipientMergeHandler.mergeRecipients(pair.first(), pair.second());
        }
        return recipientIds;
    }

    @Override
    public void storeContact(final RecipientId recipientId, final Contact contact) {
        synchronized (recipients) {
            final var recipient = recipients.get(recipientId);
            storeRecipientLocked(recipientId, Recipient.newBuilder(recipient).withContact(contact).build());
        }
    }

    @Override
    public Contact getContact(final RecipientId recipientId) {
        final var recipient = getRecipient(recipientId);
        return recipient == null ? null : recipient.getContact();
    }

    @Override
    public List<Pair<RecipientId, Contact>> getContacts() {
        return recipients.getContacts();
    }

    @Override
    public Profile getProfile(final RecipientId recipientId) {
        final var recipient = getRecipient(recipientId);
        return recipient == null ? null : recipient.getProfile();
    }

    @Override
    public ProfileKey getProfileKey(final RecipientId recipientId) {
        final var recipient = getRecipient(recipientId);
        return recipient == null ? null : recipient.getProfileKey();
    }

    @Override
    public ProfileKeyCredential getProfileKeyCredential(final RecipientId recipientId) {
        final var recipient = getRecipient(recipientId);
        return recipient == null ? null : recipient.getProfileKeyCredential();
    }

    @Override
    public void storeProfile(final RecipientId recipientId, final Profile profile) {
        synchronized (recipients) {
            final var recipient = recipients.get(recipientId);
            storeRecipientLocked(recipientId, Recipient.newBuilder(recipient).withProfile(profile).build());
        }
    }

    @Override
    public void storeProfileKey(final RecipientId recipientId, final ProfileKey profileKey) {
        synchronized (recipients) {
            final var recipient = recipients.get(recipientId);
            if (profileKey != null && profileKey.equals(recipient.getProfileKey())) {
                return;
            }

            final var newRecipient = Recipient.newBuilder(recipient)
                    .withProfileKey(profileKey)
                    .withProfileKeyCredential(null)
                    .withProfile(recipient.getProfile() == null
                            ? null
                            : Profile.newBuilder(recipient.getProfile()).withLastUpdateTimestamp(0).build())
                    .build();
            storeRecipientLocked(recipientId, newRecipient);
        }
    }

    @Override
    public void storeProfileKeyCredential(
            final RecipientId recipientId, final ProfileKeyCredential profileKeyCredential
    ) {
        synchronized (recipients) {
            final var recipient = recipients.get(recipientId);
            storeRecipientLocked(recipientId,
                    Recipient.newBuilder(recipient).withProfileKeyCredential(profileKeyCredential).build());
        }
    }

    public boolean isEmpty() {
        synchronized (recipients) {
            return recipients.isEmpty();
        }
    }

    /**
     * @param isHighTrust true, if the number/uuid connection was obtained from a trusted source.
     *                    Has no effect, if the address contains only a number or a uuid.
     */
    private RecipientId resolveRecipient(RecipientAddress address, boolean isHighTrust) {
        final Pair<RecipientId, Optional<RecipientId>> pair;
        synchronized (recipients) {
            pair = resolveRecipientLocked(address, isHighTrust);
            if (pair.second().isPresent()) {
                recipientsMerged.put(pair.second().get(), pair.first());
            }
        }

        if (pair.second().isPresent()) {
            recipientMergeHandler.mergeRecipients(pair.first(), pair.second().get());
        }
        return pair.first();
    }

    private Pair<RecipientId, Optional<RecipientId>> resolveRecipientLocked(
            RecipientAddress address, boolean isHighTrust
    ) {
        final var byNumber = address.getNumber().isEmpty()
                ? Optional.<Recipient>empty()
                : findByNumberLocked(address.getNumber().get());
        final var byUuid = address.getUuid().isEmpty()
                ? Optional.<Recipient>empty()
                : findByUuidLocked(address.getUuid().get());

        if (byNumber.isEmpty() && byUuid.isEmpty()) {
            logger.debug("Got new recipient, both uuid and number are unknown");

            if (isHighTrust || address.getUuid().isEmpty() || address.getNumber().isEmpty()) {
                return new Pair<>(addNewRecipientLocked(address), Optional.empty());
            }

            return new Pair<>(addNewRecipientLocked(new RecipientAddress(address.getUuid().get())), Optional.empty());
        }

        if (!isHighTrust || address.getUuid().isEmpty() || address.getNumber().isEmpty() || byNumber.equals(byUuid)) {
            return new Pair<>(byUuid.or(() -> byNumber).map(Recipient::getRecipientId).get(), Optional.empty());
        }

        if (byNumber.isEmpty()) {
            logger.debug("Got recipient existing with uuid, updating with high trust number");
            updateRecipientAddressLocked(byUuid.get().getRecipientId(), address);
            return new Pair<>(byUuid.get().getRecipientId(), Optional.empty());
        }

        if (byUuid.isEmpty()) {
            if (byNumber.get().getAddress().getUuid().isPresent()) {
                logger.debug(
                        "Got recipient existing with number, but different uuid, so stripping its number and adding new recipient");

                updateRecipientAddressLocked(byNumber.get().getRecipientId(),
                        new RecipientAddress(byNumber.get().getAddress().getUuid().get()));
                return new Pair<>(addNewRecipientLocked(address), Optional.empty());
            }

            logger.debug("Got recipient existing with number and no uuid, updating with high trust uuid");
            updateRecipientAddressLocked(byNumber.get().getRecipientId(), address);
            return new Pair<>(byNumber.get().getRecipientId(), Optional.empty());
        }

        if (byNumber.get().getAddress().getUuid().isPresent()) {
            logger.debug(
                    "Got separate recipients for high trust number and uuid, recipient for number has different uuid, so stripping its number");

            updateRecipientAddressLocked(byNumber.get().getRecipientId(),
                    new RecipientAddress(byNumber.get().getAddress().getUuid().get()));
            updateRecipientAddressLocked(byUuid.get().getRecipientId(), address);
            return new Pair<>(byUuid.get().getRecipientId(), Optional.empty());
        }

        logger.debug("Got separate recipients for high trust number and uuid, need to merge them");
        updateRecipientAddressLocked(byUuid.get().getRecipientId(), address);
        mergeRecipientsLocked(byUuid.get().getRecipientId(), byNumber.get().getRecipientId());
        return new Pair<>(byUuid.get().getRecipientId(), byNumber.map(Recipient::getRecipientId));
    }

    private RecipientId addNewRecipientLocked(final RecipientAddress address) {
        final var nextRecipientId = nextIdLocked();
        storeRecipientLocked(nextRecipientId, new Recipient(nextRecipientId, address, null, null, null, null));
        return nextRecipientId;
    }

    private void updateRecipientAddressLocked(
            final RecipientId recipientId, final RecipientAddress address
    ) {
        final var recipient = recipients.get(recipientId);
        storeRecipientLocked(recipientId, Recipient.newBuilder(recipient).withAddress(address).build());
    }

    private void storeRecipientLocked(
            final RecipientId recipientId, final Recipient recipient
    ) {
        recipients.put(recipientId, recipient);
    }

    private void mergeRecipientsLocked(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        final var recipient = recipients.get(recipientId);
        final var toBeMergedRecipient = recipients.get(toBeMergedRecipientId);
        recipients.put(recipientId,
                new Recipient(recipientId,
                        recipient.getAddress(),
                        recipient.getContact() != null ? recipient.getContact() : toBeMergedRecipient.getContact(),
                        recipient.getProfileKey() != null
                                ? recipient.getProfileKey()
                                : toBeMergedRecipient.getProfileKey(),
                        recipient.getProfileKeyCredential() != null
                                ? recipient.getProfileKeyCredential()
                                : toBeMergedRecipient.getProfileKeyCredential(),
                        recipient.getProfile() != null ? recipient.getProfile() : toBeMergedRecipient.getProfile()));
        recipients.remove(toBeMergedRecipientId);
    }

    private Optional<Recipient> findByNumberLocked(final String number) {
    	return Optional.ofNullable(recipients.get(number));
    }

    private Optional<Recipient> findByUuidLocked(final UUID uuid) {
    	return Optional.ofNullable(recipients.get(uuid));
    }

    private RecipientId nextIdLocked() {
        return RecipientId.of(recipients.nextRecipientId());
    }

    public interface RecipientMergeHandler {

        void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId);
    }

	@Override
	public void close() {
		recipients.close();
	}
}
