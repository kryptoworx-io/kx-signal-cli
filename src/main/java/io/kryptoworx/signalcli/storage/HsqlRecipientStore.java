package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.asamk.signal.manager.storage.Utils;
import org.asamk.signal.manager.storage.recipients.Contact;
import org.asamk.signal.manager.storage.recipients.IRecipientStore;
import org.asamk.signal.manager.storage.recipients.Profile;
import org.asamk.signal.manager.storage.recipients.Profile.Capability;
import org.asamk.signal.manager.storage.recipients.Profile.UnidentifiedAccessMode;
import org.asamk.signal.manager.storage.recipients.Recipient;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientMergeHandler;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKey;
import org.signal.zkgroup.profiles.ProfileKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.util.Pair;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.push.exceptions.UnregisteredUserException;

public class HsqlRecipientStore extends HsqlStore implements IRecipientStore {

    private final static Logger logger = LoggerFactory.getLogger(HsqlRecipientStore.class);

    private final RecipientMergeHandler recipientMergeHandler;
    private final Object lock = new Object();

    private final Map<RecipientId, RecipientId> recipientsMerged = new HashMap<>();

    private static final String SQL_QUERY_RECIPIENT = """
            SELECT
                r.id,
                r.e164,
                r.guid,
                r.profile_key,
                r.profile_key_credential,
                r.has_contact,
                r.c_name,
                r.c_color,
                r.c_message_expiration_time,
                r.c_blocked,
                r.c_archived,
                r.has_profile,
                r.p_last_update_timestamp,
                r.p_given_name,
                r.p_family_name,
                r.p_about,
                r.p_about_emoji,
                r.p_unidentified_access_mode,
                r.p_capabilities
            FROM recipient r
            """;

    public HsqlRecipientStore(SQLConnectionFactory connectionFactory, RecipientMergeHandler recipientMergeHandler) {
        super(connectionFactory);
        this.recipientMergeHandler = recipientMergeHandler;
        voidTransaction(this::initialize);
    }

    public RecipientAddress resolveRecipientAddress(RecipientId recipientId) {
        synchronized (lock) {
            return getRecipient(recipientId).getAddress();
        }
    }

    public Recipient getRecipient(RecipientId recipientId) {
        synchronized (lock) {
            return transaction(c -> dbLoadRecipient(c, resolveRecipientId(recipientId)));
        }
    }

    private RecipientId resolveRecipientId(RecipientId recipientId) {
        while (recipientsMerged.containsKey(recipientId)) {
            recipientId = recipientsMerged.get(recipientId);
        }
        return recipientId;
    }

    private Recipient dbLoadRecipient(Connection connection, RecipientId recipientId) throws SQLException {
        return dbLoadRecipient(connection, recipientId.getId());
    }

    private Recipient dbLoadRecipient(Connection connection, long recipientId) throws SQLException {
        String sqlQuery = SQL_QUERY_RECIPIENT + " WHERE r.id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setLong(1, recipientId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? readRecipient(rs) : null;
            }
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
        synchronized (lock) {
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
        synchronized (lock) {
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
        synchronized (lock) {
            voidTransaction(c -> dbUpdateContact(c, recipientId.getId(), contact));
        }
    }

    private void dbUpdateContact(Connection connection, long recipientId, Contact contact) throws SQLException {
        var updateContact = SQLUpdate.forTable("recipient", "r");
        if (contact != null) {
            updateContact
                .set("has_contact", SQLArgument.of(true))
                .set("c_message_expiration_time", SQLArgument.of(contact.getMessageExpirationTime()))
                .set("c_blocked", SQLArgument.of(contact.isBlocked()))
                .set("c_archived", SQLArgument.of(contact.isArchived()))
                ;
            if (contact.getName() != null) {
                updateContact.set("c_name", SQLArgument.of(contact.getName()));
            }
            if (contact.getColor() != null) {
                updateContact.set("c_color", SQLArgument.of(contact.getColor()));
            }
        } else {
            updateContact.set("has_contact", SQLArgument.of(false));
        }
        updateContact
                .where("r.id = ? ", SQLArgument.of(recipientId))
                .execute(connection);
    }

    @Override
    public Contact getContact(final RecipientId recipientId) {
        final var recipient = getRecipient(recipientId);
        return recipient == null ? null : recipient.getContact();
    }

    @Override
    public List<Pair<RecipientId, Contact>> getContacts() {
        return transaction(this::loadContacts);
    }

    private List<Pair<RecipientId, Contact>> loadContacts(Connection connection) throws SQLException {
        String sqlQuery = SQL_QUERY_RECIPIENT + " WHERE r.has_contact";
        List<Pair<RecipientId, Contact>> result = new ArrayList<>();
        try (var stmt = connection.prepareStatement(sqlQuery);
                ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                long recipientId = rs.getLong(1);
                Contact contact = readContact(rs, 7);
                result.add(new Pair<>(RecipientId.of(recipientId), contact));
            }
        }
        return result;
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
        synchronized (lock) {
            voidTransaction(c -> dbUpdateProfile(c, recipientId.getId(), profile));
        }
    }

    private void dbUpdateProfile(Connection connection, long recipientId, Profile profile) throws SQLException {
        var updateProfile = SQLUpdate.forTable("recipient", "r")
                .set("has_profile", SQLArgument.of(true))
                .set("p_last_update_timestamp", SQLArgument.of(profile.getLastUpdateTimestamp()))
                .set("p_unidentified_access_mode", 
                        SQLArgument.of(profile.getUnidentifiedAccessMode().ordinal()))
                .set("p_capabilities", SQLArgument.of(EnumUtil.toInt(profile.getCapabilities())))
                .where("r.id = ? ", SQLArgument.of(recipientId))
                ;
        if (profile.getGivenName() != null) {
            updateProfile.set("p_given_name", SQLArgument.of(profile.getGivenName()));
        }
        if (profile.getFamilyName() != null) {
            updateProfile.set("p_family_name", SQLArgument.of(profile.getFamilyName()));
        }
        if (profile.getAbout() != null) {
            updateProfile.set("p_about", SQLArgument.of(profile.getAbout()));
        }
        if (profile.getAboutEmoji() != null) {
            updateProfile.set("p_about_emoji", SQLArgument.of(profile.getAboutEmoji()));
        }
        updateProfile.execute(connection);
    }

    @Override
    public void storeProfileKey(final RecipientId recipientId, final ProfileKey profileKey) {
        synchronized (lock) {
            voidTransaction(c -> dbUpdateProfileKey(c, recipientId.getId(), profileKey));
        }
    }

    private void dbUpdateProfileKey(Connection connection, long recipientId, ProfileKey profileKey) throws SQLException {
        byte[] profileKeyBytes = profileKey != null ? profileKey.serialize() : null;
        SQLUpdate updateRecipient = SQLUpdate.forTable("recipient", "r")
                .set("profile_key", SQLArgument.of(profileKeyBytes))
                .set("profile_key_credential", SQLArgument.of((byte[]) null))
                .set("p_last_update_timestamp", SQLArgument.of(0L));
        if (profileKey != null) {
            updateRecipient.where("r.id = ? AND (r.profile_key IS NULL OR r.profile_key <> ?)",
                    SQLArgument.of(recipientId),
                    SQLArgument.of(profileKeyBytes));
        } else {
            updateRecipient.where("r.id = ? AND r.profile_key IS NOT NULL",
                    SQLArgument.of(recipientId));
        }
        updateRecipient.execute(connection);
    }

    @Override
    public void storeProfileKeyCredential(
            final RecipientId recipientId, final ProfileKeyCredential profileKeyCredential
    ) {
        synchronized (lock) {
            voidTransaction(c -> dbUpdateProfileKeyCredential(c, recipientId.getId(), profileKeyCredential));
        }
    }

    private void dbUpdateProfileKeyCredential(Connection connection, long recipientId, ProfileKeyCredential profileKeyCredential) throws SQLException {
        byte[] profileKeyCredentialBytes = profileKeyCredential.serialize();
        SQLUpdate.forTable("recipient", "r")
                .set("profile_key_credential", SQLArgument.of(profileKeyCredentialBytes))
                .where("r.id = ?", SQLArgument.of(recipientId))
                .execute(connection);
    }

    public boolean isEmpty() {
        synchronized (lock) {
            return transaction(this::dbIsEmpty);
        }
    }

    private boolean dbIsEmpty(Connection connection) throws SQLException {
        String sqlQuery = "SELECT COUNT(*) FROM recipient";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery);
             ResultSet rs = stmt.executeQuery()) {
            if (!rs.next()) throw new AssertionError();
            return rs.getInt(1) == 0;
        }
    }

    /**
     * @param isHighTrust true, if the number/uuid connection was obtained from a trusted source.
     *                    Has no effect, if the address contains only a number or a uuid.
     */
    private RecipientId resolveRecipient(RecipientAddress address, boolean isHighTrust) {
        final Pair<RecipientId, Optional<RecipientId>> pair;
        synchronized (lock) {
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
        mergeRecipientsLocked(byUuid.get(), byNumber.get());
        return new Pair<>(byUuid.get().getRecipientId(), byNumber.map(Recipient::getRecipientId));
    }

    private RecipientId addNewRecipientLocked(final RecipientAddress address) {
        return transaction(c -> dbAddNewRecipient(c, address.getNumber(), address.getUuid()));
    }

    private RecipientId dbAddNewRecipient(Connection connection, Optional<String> e164, Optional<UUID> uuid) throws SQLException {
        SQLInsert insertRecipient = SQLInsert.forTable("recipient");
        if (e164.isPresent()) insertRecipient.add("e164", e164.get(), PreparedStatement::setString);
        if (uuid.isPresent()) insertRecipient.add("guid", uuid.get(), PreparedStatement::setObject);
        return insertRecipient.execute(connection,
                rs -> RecipientId.of(rs.getLong(1)));
    }

    private void updateRecipientAddressLocked(
            final RecipientId recipientId, final RecipientAddress address
    ) {
        voidTransaction(c -> dbUpdateAddress(c, recipientId.getId(), address));
    }

    private void dbUpdateAddress(Connection connection, long recipientId, RecipientAddress address) throws SQLException {
        Optional<String> number = address.getNumber();
        Optional<UUID> uuid = address.getUuid();
        SQLUpdate.forTable("recipient", "r")
                .set("e164", SQLArgument.of(number.orElse(null)))
                .set("guid", SQLArgument.of(uuid.orElse(null)))
                .where("r.id = ?", SQLArgument.of(recipientId))
                .execute(connection);
    }

    private void mergeRecipientsLocked(Recipient recipient, Recipient toBeMergedRecipient) {
        long recipientId = recipient.getRecipientId().getId();
        long toBeMergedRecipientId = toBeMergedRecipient.getRecipientId().getId();
        voidTransaction(c -> dbMergeRecipients(c, recipientId, toBeMergedRecipientId));
    }

    private void dbMergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        recipientMergeHandler.beforeMergeRecipients(connection, recipientId, toBeMergedRecipientId);
        // Unfortunately, using ORDER BY in the query prevents to ResultSet from being
        // updateable, so we don't know which row will be returned first
        String sqlQuery = SQL_QUERY_RECIPIENT + " WHERE r.id IN (?, ?) FOR UPDATE";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)) {
            stmt.setLong(1, recipientId);
            stmt.setLong(2, toBeMergedRecipientId);
            try (ResultSet rs = stmt.executeQuery()) {
                Contact contact = null;
                Profile profile = null;
                byte[] profileKeyBytes = null, profileKeyCredBytes = null;
                
                int recipientRowIndex = 2; 

                rs.next();
                long id = rs.getLong(1);
                
                if (id != toBeMergedRecipientId) {
                    rs.next();
                    recipientRowIndex = 1;
                }

                int p = 4;
                profileKeyBytes = rs.getBytes(p++);
                profileKeyCredBytes = rs.getBytes(p++);
                boolean hasContact = rs.getBoolean(p++);
                if (hasContact) {
                    contact = new Contact(
                            rs.getString(p++),   // name
                            rs.getString(p++),   // color
                            rs.getInt(p++),      // message expiration
                            rs.getBoolean(p++),  // blocked
                            rs.getBoolean(p++)   // archived
                            );
                } else {
                    p += 5;
                }
                boolean hasProfile = rs.getBoolean(p++);
                if (hasProfile) {
                    profile = new Profile(
                            rs.getLong(p++), // last update
                            rs.getString(p++), // given name
                            rs.getString(p++), // family name
                            rs.getString(p++), // about
                            rs.getString(p++), // about emoji
                            EnumUtil.fromOrdinal(UnidentifiedAccessMode.class, rs.getInt(p++)), // unidentified access mode
                            EnumUtil.fromInt(Capability.class, rs.getInt(p++)) // capabilities
                            );
                }
                rs.deleteRow();
                
                rs.absolute(recipientRowIndex);
                p = 4;
                if (rs.getBytes(p) == null && profileKeyBytes != null) {
                    rs.updateBytes(p, profileKeyBytes);
                }
                if (rs.getBytes(++p) == null && profileKeyCredBytes != null) {
                    rs.updateBytes(p, profileKeyCredBytes);
                }
                hasContact = rs.getBoolean(++p);
                if (!hasContact && contact != null) {
                    rs.updateBoolean(p++, true);
                    rs.updateString(p++, contact.getName());
                    rs.updateString(p++, contact.getColor());
                    rs.updateInt(p++, contact.getMessageExpirationTime());
                    rs.updateBoolean(p++, contact.isBlocked());
                    rs.updateBoolean(p++, contact.isArchived());
                } else {
                    p += 6;
                }
                hasProfile = rs.getBoolean(p);
                if (!hasProfile && profile != null) {
                    rs.updateBoolean(p++, true);
                    rs.updateLong(p++, profile.getLastUpdateTimestamp());
                    rs.updateString(p++, profile.getGivenName());
                    rs.updateString(p++, profile.getFamilyName());
                    rs.updateString(p++, profile.getAbout());
                    rs.updateString(p++, profile.getAboutEmoji());
                    rs.updateInt(p++, profile.getUnidentifiedAccessMode().ordinal());
                    rs.updateInt(p++, EnumUtil.toInt(profile.getCapabilities()));
                }
                rs.updateRow();
            }
        }
    }

    private Optional<Recipient> findByNumberLocked(final String number) {
        return transaction(c -> dbQueryRecipient(c, "e164", number, PreparedStatement::setString));
    }

    private Optional<Recipient> findByUuidLocked(final UUID uuid) {
        return transaction(c -> dbQueryRecipient(c, "guid", uuid, PreparedStatement::setObject));
    }

    private <T> Optional<Recipient> dbQueryRecipient(Connection connection, String column, T value, SQLParameterSetter<T> parameterSetter) throws SQLException {
        String sqlQuery = SQL_QUERY_RECIPIENT + " WHERE r." + column + " = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            parameterSetter.setValue(stmt, 1, value);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(readRecipient(rs));
            }
        }
    }

    private static Contact readContact(ResultSet rs, int col) throws SQLException {
        var contacBuilder = Contact.newBuilder();
        readContact(rs, col, contacBuilder);
        return contacBuilder.build();
    }

    private static int readContact(ResultSet rs, int col, Contact.Builder contactBuilder) throws SQLException {
        contactBuilder
                .withName(rs.getString(col++))
                .withColor(rs.getString(col++))
                .withMessageExpirationTime(rs.getInt(col++))
                .withBlocked(rs.getBoolean(col++))
                .withArchived(rs.getBoolean(col++))
                .build();
        return col;
    }

    private static Recipient readRecipient(ResultSet rs) throws SQLException {
        int col = 1;
        RecipientId id = RecipientId.of(rs.getLong(col++));
        var e164 = Optional.ofNullable(rs.getString(col++));
        var uuid = Optional.ofNullable(rs.getObject(col++)).map(UUID.class::cast);
        RecipientAddress addr = new RecipientAddress(uuid, e164);
        byte[] profileKeyBytes = rs.getBytes(col++);
        byte[] profileKeyCredBytes = rs.getBytes(col++);
        Contact contact = null;
        boolean hasContact = rs.getBoolean(col++);
        if (hasContact) {
            var contactBuilder = Contact.newBuilder();
            col = readContact(rs, col, contactBuilder);
            contact = contactBuilder.build();
        } else {
            col += 5;
        }
        Profile profile = null;
        boolean hasProfile = rs.getBoolean(col++);
        if (hasProfile) {
            profile = Profile.newBuilder()
                    .withLastUpdateTimestamp(rs.getLong(col++))
                    .withGivenName(rs.getString(col++))
                    .withFamilyName(rs.getString(col++))
                    .withAbout(rs.getString(col++))
                    .withAboutEmoji(rs.getString(col++))
                    .withUnidentifiedAccessMode(EnumUtil.fromOrdinal(Profile.UnidentifiedAccessMode.class, rs.getInt(col++)))
                    .withCapabilities(EnumUtil.fromInt(Profile.Capability.class, rs.getInt(col++)))
                    .build();
        } else {
            col += 7;
        }
        var recipientBuilder = Recipient.newBuilder()
                .withRecipientId(id)
                .withAddress(addr);
        if (profileKeyBytes != null) {
            recipientBuilder.withProfileKey(profileKeyFromBytes(profileKeyBytes));
        }
        if (profileKeyCredBytes != null) {
            recipientBuilder.withProfileKeyCredential(profileKeyCredentialFromBytes(profileKeyCredBytes));
        }
        if (contact != null) {
            recipientBuilder.withContact(contact);
        }
        if (profile != null) {
            recipientBuilder.withProfile(profile);
        }
        return recipientBuilder.build();
    }

    private static ProfileKey profileKeyFromBytes(byte[] bytes) {
        try {
            return new ProfileKey(bytes);
        } catch (InvalidInputException e) {
            throw new AssertionError("Failed to deserialize ProfileKey");
        }
    }

    private static ProfileKeyCredential profileKeyCredentialFromBytes(byte[] bytes) {
        try {
            return new ProfileKeyCredential(bytes);
        } catch (InvalidInputException e) {
            throw new AssertionError("Failed to deserialize ProfileKeyCredential");
        }
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateRecipientTable = """
            CREATE TABLE IF NOT EXISTS recipient
            (
                id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1) PRIMARY KEY NOT NULL,
                e164 VARCHAR(20 characters),
                guid UUID,
                profile_key BINARY(32),
                profile_key_credential BINARY(145),
                has_contact BOOLEAN DEFAULT FALSE NOT NULL,
                c_name VARCHAR(50 CHARACTERS),
                c_color VARCHAR(20 CHARACTERS),
                c_message_expiration_time INTEGER DEFAULT 0 NOT NULL,
                c_blocked BOOLEAN DEFAULT FALSE NOT NULL,
                c_archived BOOLEAN DEFAULT FALSE NOT NULL,
                has_profile BOOLEAN DEFAULT FALSE NOT NULL,
                p_last_update_timestamp BIGINT,
                p_given_name varchar(50 CHARACTERS),
                p_family_name varchar(50 CHARACTERS),
                p_about VARCHAR(200 CHARACTERS),
                p_about_emoji VARCHAR(200 CHARACTERS),
                p_unidentified_access_mode TINYINT,
                p_capabilities INT
            )
            """;
        String sqlCreateIndexE164 = "CREATE INDEX IF NOT EXISTS ix_rcpt_guid ON recipient(e164)";
        String sqlCreateIndexUUID = "CREATE INDEX IF NOT EXISTS ix_rcpt_e164 ON recipient(guid)";
        try (var stmt = connection.prepareStatement(sqlCreateRecipientTable)) {
            stmt.execute();
        }
        try (var stmt = connection.prepareStatement(sqlCreateIndexE164)) {
            stmt.execute();
        }
        try (var stmt = connection.prepareStatement(sqlCreateIndexUUID)) {
            stmt.execute();
        }
    }
}
