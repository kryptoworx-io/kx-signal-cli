package org.asamk.signal.manager.storage;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;

import org.apache.commons.codec.binary.Hex;
import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.groups.GroupId;
import org.asamk.signal.manager.storage.contacts.ContactsStore;
import org.asamk.signal.manager.storage.contacts.LegacyJsonContactsStore;
import org.asamk.signal.manager.storage.groups.GroupInfoV1;
import org.asamk.signal.manager.storage.groups.GroupStore;
import org.asamk.signal.manager.storage.identities.IdentityKeyStore;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.manager.storage.messageCache.MessageCache;
import org.asamk.signal.manager.storage.prekeys.PreKeyStore;
import org.asamk.signal.manager.storage.prekeys.SignedPreKeyStore;
import org.asamk.signal.manager.storage.profiles.LegacyProfileStore;
import org.asamk.signal.manager.storage.profiles.ProfileStore;
import org.asamk.signal.manager.storage.protocol.LegacyJsonSignalProtocolStore;
import org.asamk.signal.manager.storage.protocol.SignalProtocolStore;
import org.asamk.signal.manager.storage.recipients.Contact;
import org.asamk.signal.manager.storage.recipients.LegacyRecipientStore;
import org.asamk.signal.manager.storage.recipients.Profile;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientStore;
import org.asamk.signal.manager.storage.senderKeys.SenderKeyStore;
import org.asamk.signal.manager.storage.sessions.SessionStore;
import org.asamk.signal.manager.storage.stickers.StickerStore;
import org.asamk.signal.manager.storage.threads.LegacyJsonThreadStore;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.KeyUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.signal.libsignal.crypto.jce.Mac;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.IdentityKeyPair;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.state.PreKeyRecord;
import org.whispersystems.libsignal.state.SessionRecord;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;
import org.whispersystems.libsignal.util.Medium;
import org.whispersystems.libsignal.util.Pair;
import org.whispersystems.signalservice.api.crypto.UnidentifiedAccess;
import org.whispersystems.signalservice.api.kbs.MasterKey;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.storage.StorageKey;
import org.whispersystems.signalservice.api.util.UuidUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kryptoworx.signalcli.storage.CipherStreamFactory;
import io.kryptoworx.signalcli.storage.NonCloseableOutputStream;

public class SignalAccount implements Closeable {

    private final static Logger logger = LoggerFactory.getLogger(SignalAccount.class);
    private static final String HMAC = "HmacSHA512/256";
    private final static UUID ID_ACCOUNT = UUID.fromString("7942598d-c34b-495a-b0cb-383766f6ba6f");
    private final static UUID ID_DATABASE = UUID.fromString("bc6bc781-f8e8-4636-849c-69de4079bea3");
    
    
    private static final int MINIMUM_STORAGE_VERSION = 1;
    private static final int CURRENT_STORAGE_VERSION = 2;

    private final ObjectMapper jsonProcessor = Utils.createStorageObjectMapper();

    private final FileChannel fileChannel;
    private final FileLock lock;
    private final DataSource dataSource;

    private String username;
    private UUID uuid;
    private String encryptedDeviceName;
    private int deviceId = SignalServiceAddress.DEFAULT_DEVICE_ID;
    private boolean isMultiDevice = false;
    private String password;
    private String registrationLockPin;
    private MasterKey pinMasterKey;
    private StorageKey storageKey;
    private long storageManifestVersion = -1;
    private ProfileKey profileKey;
    private int preKeyIdOffset;
    private int nextSignedPreKeyId;
    private long lastReceiveTimestamp = 0;

    private boolean registered = false;

    private SignalProtocolStore signalProtocolStore;
    private PreKeyStore preKeyStore;
    private SignedPreKeyStore signedPreKeyStore;
    private SessionStore sessionStore;
    private IdentityKeyStore identityKeyStore;
    private SenderKeyStore senderKeyStore;
    private GroupStore groupStore;
    private GroupStore.Storage groupStoreStorage;
    private RecipientStore recipientStore;
    private StickerStore stickerStore;
    private StickerStore.Storage stickerStoreStorage;
    private final CipherStreamFactory cipherStreamFactory;

    private MessageCache messageCache;

    private SignalAccount(final DataSource dataSource, final FileChannel fileChannel, final FileLock lock, byte[] masterKey) {
    	this.dataSource = dataSource;
        this.fileChannel = fileChannel;
        this.lock = lock;
    	this.cipherStreamFactory = masterKey != null ? new CipherStreamFactory(ID_ACCOUNT, masterKey) : null;
    }

    public static SignalAccount load(
            File dataPath, String username, boolean waitForLock, final TrustNewIdentity trustNewIdentity,
            byte[] masterKey
    ) throws IOException {
    	DataSource dataSource = createDataSource(dataPath, username, masterKey);
        final var fileName = getFileName(dataPath, username);
        final var pair = openFileChannel(fileName, waitForLock);
        try {
            var account = new SignalAccount(dataSource, pair.first(), pair.second(), masterKey);
            account.load(username, dataPath, trustNewIdentity, masterKey);
            account.migrateLegacyConfigs();

            return account;
        } catch (Throwable e) {
            pair.second().close();
            pair.first().close();
            throw e;
        }
    }
    
    private static DataSource createDataSource(File dataPath, String username, byte[] masterKey) {
    	String dbPassword = "";
    	if (masterKey != null && masterKey.length > 0) {
	    	try {
	    		Mac hmac = Mac.getInstance(HMAC);
	    		SecretKeySpec secretKey = new SecretKeySpec(masterKey, HMAC);
	    		hmac.init(secretKey);
	    		byte[] idBytes = new byte[16];
	    		ByteBuffer idBuf = ByteBuffer.wrap(idBytes);
	    		idBuf.putLong(ID_DATABASE.getMostSignificantBits());
	    		idBuf.putLong(ID_DATABASE.getLeastSignificantBits());
	    		byte[] dbKey = hmac.doFinal(idBytes);
	    		dbPassword = Hex.encodeHexString(dbKey) + " ";
	    	} catch (GeneralSecurityException e) {
	    		throw new AssertionError("Failed to derive database key", e);
	    	}
    	}
    	File dbFile = getDatabaseFile(dataPath, username);
    	String databaseUrl = "jdbc:h2:file:" + dbFile;
    	if (!dbPassword.isEmpty()) databaseUrl += ";cipher=AES";
		JdbcConnectionPool jdbcPool = JdbcConnectionPool.create(databaseUrl, "", dbPassword);
		jdbcPool.setMaxConnections(4);
		return jdbcPool;
    }

    public static SignalAccount create(
            File dataPath,
            String username,
            IdentityKeyPair identityKey,
            int registrationId,
            ProfileKey profileKey,
            final TrustNewIdentity trustNewIdentity,
            final byte[] masterKey
    ) throws IOException {
        IOUtils.createPrivateDirectories(dataPath);
        var fileName = getFileName(dataPath, username);
        if (!fileName.exists()) {
            IOUtils.createPrivateFile(fileName);
        }

        final var pair = openFileChannel(fileName, true);
        var account = new SignalAccount(createDataSource(dataPath, username, masterKey), pair.first(), pair.second(), masterKey);

        account.username = username;
        account.profileKey = profileKey;

        account.initStores(dataPath, identityKey, registrationId, trustNewIdentity, masterKey);
        account.groupStore = new GroupStore(getGroupCachePath(dataPath, username),
                account.recipientStore,
                account::saveGroupStore);
        account.stickerStore = new StickerStore(account::saveStickerStore);

        account.registered = false;

        account.migrateLegacyConfigs();
        account.save();

        return account;
    }

    private void initStores(
            final File dataPath,
            final IdentityKeyPair identityKey,
            final int registrationId,
            final TrustNewIdentity trustNewIdentity,
            final byte[] masterKey
    ) throws IOException {
        recipientStore = RecipientStore.load(dataSource, this::mergeRecipients);
        preKeyStore = new PreKeyStore(dataSource);
        signedPreKeyStore = new SignedPreKeyStore(dataSource);
        sessionStore = new SessionStore(dataSource, recipientStore);
        identityKeyStore = new IdentityKeyStore(dataSource,
                recipientStore,
                identityKey,
                registrationId,
                trustNewIdentity,
                masterKey);
        senderKeyStore = new SenderKeyStore(getSharedSenderKeysFile(dataPath, username),
                getSenderKeysPath(dataPath, username),
                recipientStore::resolveRecipientAddress,
                recipientStore);
        signalProtocolStore = new SignalProtocolStore(preKeyStore,
                signedPreKeyStore,
                sessionStore,
                identityKeyStore,
                senderKeyStore,
                this::isMultiDevice);

        messageCache = new MessageCache(getMessageCachePath(dataPath, username));
    }

    public static SignalAccount createOrUpdateLinkedAccount(
            File dataPath,
            String username,
            UUID uuid,
            String password,
            String encryptedDeviceName,
            int deviceId,
            IdentityKeyPair identityKey,
            int registrationId,
            ProfileKey profileKey,
            final TrustNewIdentity trustNewIdentity,
            final byte[] masterKey
    ) throws IOException {
        IOUtils.createPrivateDirectories(dataPath);
        var fileName = getFileName(dataPath, username);
        if (!fileName.exists()) {
            return createLinkedAccount(dataPath,
                    username,
                    uuid,
                    password,
                    encryptedDeviceName,
                    deviceId,
                    identityKey,
                    registrationId,
                    profileKey,
                    trustNewIdentity,
                    masterKey);
        }

        final var account = load(dataPath, username, true, trustNewIdentity, masterKey);
        account.setProvisioningData(username, uuid, password, encryptedDeviceName, deviceId, profileKey);
        account.recipientStore.resolveRecipientTrusted(account.getSelfAddress());
        account.sessionStore.archiveAllSessions();
        account.senderKeyStore.deleteAll();
        account.clearAllPreKeys();
        return account;
    }

    private void clearAllPreKeys() {
        this.preKeyIdOffset = 0;
        this.nextSignedPreKeyId = 0;
        this.preKeyStore.removeAllPreKeys();
        this.signedPreKeyStore.removeAllSignedPreKeys();
        save();
    }

    private static SignalAccount createLinkedAccount(
            File dataPath,
            String username,
            UUID uuid,
            String password,
            String encryptedDeviceName,
            int deviceId,
            IdentityKeyPair identityKey,
            int registrationId,
            ProfileKey profileKey,
            final TrustNewIdentity trustNewIdentity,
            final byte[] masterKey
    ) throws IOException {
        var fileName = getFileName(dataPath, username);
        IOUtils.createPrivateFile(fileName);

        final var pair = openFileChannel(fileName, true);
        var account = new SignalAccount(createDataSource(dataPath, username, masterKey), pair.first(), pair.second(), masterKey);

        account.setProvisioningData(username, uuid, password, encryptedDeviceName, deviceId, profileKey);

        account.initStores(dataPath, identityKey, registrationId, trustNewIdentity, masterKey);
        account.groupStore = new GroupStore(getGroupCachePath(dataPath, username),
                account.recipientStore,
                account::saveGroupStore);
        account.stickerStore = new StickerStore(account::saveStickerStore);

        account.recipientStore.resolveRecipientTrusted(account.getSelfAddress());
        account.migrateLegacyConfigs();
        account.save();

        return account;
    }

    private void setProvisioningData(
            final String username,
            final UUID uuid,
            final String password,
            final String encryptedDeviceName,
            final int deviceId,
            final ProfileKey profileKey
    ) {
        this.username = username;
        this.uuid = uuid;
        this.password = password;
        this.profileKey = profileKey;
        this.encryptedDeviceName = encryptedDeviceName;
        this.deviceId = deviceId;
        this.registered = true;
        this.isMultiDevice = true;
        this.lastReceiveTimestamp = 0;
        this.pinMasterKey = null;
        this.storageManifestVersion = -1;
        this.storageKey = null;
    }

    private void migrateLegacyConfigs() {
        if (getPassword() == null) {
            setPassword(KeyUtils.createPassword());
        }

        if (getProfileKey() == null && isRegistered()) {
            // Old config file, creating new profile key
            setProfileKey(KeyUtils.createProfileKey());
        }
        // Ensure our profile key is stored in profile store
        getProfileStore().storeProfileKey(getSelfRecipientId(), getProfileKey());
    }

    private void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
        sessionStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        identityKeyStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        messageCache.mergeRecipients(recipientId, toBeMergedRecipientId);
        groupStore.mergeRecipients(recipientId, toBeMergedRecipientId);
        senderKeyStore.mergeRecipients(recipientId, toBeMergedRecipientId);
    }

    public static File getFileName(File dataPath, String username) {
        return new File(dataPath, username);
    }

    private static File getUserPath(final File dataPath, final String username) {
        final var path = new File(dataPath, username + ".d");
        try {
            IOUtils.createPrivateDirectories(path);
        } catch (IOException e) {
            throw new AssertionError("Failed to create user path", e);
        }
        return path;
    }

    private static File getMessageCachePath(File dataPath, String username) {
        return new File(getUserPath(dataPath, username), "msg-cache");
    }

    private static File getGroupCachePath(File dataPath, String username) {
        return new File(getUserPath(dataPath, username), "group-cache");
    }

    private static File getSenderKeysPath(File dataPath, String username) {
        return new File(getUserPath(dataPath, username), "sender-keys");
    }

    private static File getSharedSenderKeysFile(File dataPath, String username) {
        return new File(getUserPath(dataPath, username), "shared-sender-keys-store");
    }

    private static File getDatabaseFile(File dataPath, String username) {
        return new File(getUserPath(dataPath, username), "storage");
    }

    public static boolean userExists(File dataPath, String username) {
        if (username == null) {
            return false;
        }
        var f = getFileName(dataPath, username);
        return !(!f.exists() || f.isDirectory());
    }

    private void load(
            String username, File dataPath, final TrustNewIdentity trustNewIdentity, byte[] masterKey
    ) throws IOException {
        JsonNode rootNode;
        synchronized (fileChannel) {
            fileChannel.position(0);
            rootNode = jsonProcessor.readTree(createInputStream());
        }

        if (rootNode.hasNonNull("version")) {
            var accountVersion = rootNode.get("version").asInt(1);
            if (accountVersion > CURRENT_STORAGE_VERSION) {
                throw new IOException("Config file was created by a more recent version!");
            } else if (accountVersion < MINIMUM_STORAGE_VERSION) {
                throw new IOException("Config file was created by a no longer supported older version!");
            }
        }

        String usernameFromFile = Utils.getNotNullNode(rootNode, "username").asText();
        if (!username.equals(usernameFromFile)) {
            throw new IOException("Username in account file doesn't match expected number: "
                    + username);
        }
        
        this.username = username;
        password = Utils.getNotNullNode(rootNode, "password").asText();
        registered = Utils.getNotNullNode(rootNode, "registered").asBoolean();
        if (rootNode.hasNonNull("uuid")) {
            try {
                uuid = UUID.fromString(rootNode.get("uuid").asText());
            } catch (IllegalArgumentException e) {
                throw new IOException("Config file contains an invalid uuid, needs to be a valid UUID", e);
            }
        }
        if (rootNode.hasNonNull("deviceName")) {
            encryptedDeviceName = rootNode.get("deviceName").asText();
        }
        if (rootNode.hasNonNull("deviceId")) {
            deviceId = rootNode.get("deviceId").asInt();
        }
        if (rootNode.hasNonNull("isMultiDevice")) {
            isMultiDevice = rootNode.get("isMultiDevice").asBoolean();
        }
        if (rootNode.hasNonNull("lastReceiveTimestamp")) {
            lastReceiveTimestamp = rootNode.get("lastReceiveTimestamp").asLong();
        }
        int registrationId = 0;
        if (rootNode.hasNonNull("registrationId")) {
            registrationId = rootNode.get("registrationId").asInt();
        }
        IdentityKeyPair identityKeyPair = null;
        if (rootNode.hasNonNull("identityPrivateKey") && rootNode.hasNonNull("identityKey")) {
            final var publicKeyBytes = Base64.getDecoder().decode(rootNode.get("identityKey").asText());
            final var privateKeyBytes = Base64.getDecoder().decode(rootNode.get("identityPrivateKey").asText());
            identityKeyPair = KeyUtils.getIdentityKeyPair(publicKeyBytes, privateKeyBytes);
        }

        if (rootNode.hasNonNull("registrationLockPin")) {
            registrationLockPin = rootNode.get("registrationLockPin").asText();
        }
        if (rootNode.hasNonNull("pinMasterKey")) {
            pinMasterKey = new MasterKey(Base64.getDecoder().decode(rootNode.get("pinMasterKey").asText()));
        }
        if (rootNode.hasNonNull("storageKey")) {
            storageKey = new StorageKey(Base64.getDecoder().decode(rootNode.get("storageKey").asText()));
        }
        if (rootNode.hasNonNull("storageManifestVersion")) {
            storageManifestVersion = rootNode.get("storageManifestVersion").asLong();
        }
        if (rootNode.hasNonNull("preKeyIdOffset")) {
            preKeyIdOffset = rootNode.get("preKeyIdOffset").asInt(0);
        } else {
            preKeyIdOffset = 0;
        }
        if (rootNode.hasNonNull("nextSignedPreKeyId")) {
            nextSignedPreKeyId = rootNode.get("nextSignedPreKeyId").asInt();
        } else {
            nextSignedPreKeyId = 0;
        }
        if (rootNode.hasNonNull("profileKey")) {
            try {
                profileKey = new ProfileKey(Base64.getDecoder().decode(rootNode.get("profileKey").asText()));
            } catch (InvalidInputException e) {
                throw new IOException(
                        "Config file contains an invalid profileKey, needs to be base64 encoded array of 32 bytes",
                        e);
            }
        }

        var migratedLegacyConfig = false;
        final var legacySignalProtocolStore = rootNode.hasNonNull("axolotlStore")
                ? jsonProcessor.convertValue(Utils.getNotNullNode(rootNode, "axolotlStore"),
                LegacyJsonSignalProtocolStore.class)
                : null;
        if (legacySignalProtocolStore != null && legacySignalProtocolStore.getLegacyIdentityKeyStore() != null) {
            identityKeyPair = legacySignalProtocolStore.getLegacyIdentityKeyStore().getIdentityKeyPair();
            registrationId = legacySignalProtocolStore.getLegacyIdentityKeyStore().getLocalRegistrationId();
            migratedLegacyConfig = true;
        }

        initStores(dataPath, identityKeyPair, registrationId, trustNewIdentity, masterKey);

        migratedLegacyConfig = loadLegacyStores(rootNode, legacySignalProtocolStore) || migratedLegacyConfig;

        if (rootNode.hasNonNull("groupStore")) {
            groupStoreStorage = jsonProcessor.convertValue(rootNode.get("groupStore"), GroupStore.Storage.class);
            groupStore = GroupStore.fromStorage(groupStoreStorage,
                    getGroupCachePath(dataPath, username),
                    recipientStore,
                    this::saveGroupStore);
        } else {
            groupStore = new GroupStore(getGroupCachePath(dataPath, username), recipientStore, this::saveGroupStore);
        }

        if (rootNode.hasNonNull("stickerStore")) {
            stickerStoreStorage = jsonProcessor.convertValue(rootNode.get("stickerStore"), StickerStore.Storage.class);
            stickerStore = StickerStore.fromStorage(stickerStoreStorage, this::saveStickerStore);
        } else {
            stickerStore = new StickerStore(this::saveStickerStore);
        }

        migratedLegacyConfig = loadLegacyThreadStore(rootNode) || migratedLegacyConfig;

        if (migratedLegacyConfig) {
            save();
        }
    }

    private boolean loadLegacyStores(
            final JsonNode rootNode, final LegacyJsonSignalProtocolStore legacySignalProtocolStore
    ) {
        var migrated = false;
        var legacyRecipientStoreNode = rootNode.get("recipientStore");
        if (legacyRecipientStoreNode != null) {
            logger.debug("Migrating legacy recipient store.");
            var legacyRecipientStore = jsonProcessor.convertValue(legacyRecipientStoreNode, LegacyRecipientStore.class);
            if (legacyRecipientStore != null) {
                recipientStore.resolveRecipientsTrusted(legacyRecipientStore.getAddresses());
            }
            recipientStore.resolveRecipientTrusted(getSelfAddress());
            migrated = true;
        }

        if (legacySignalProtocolStore != null && legacySignalProtocolStore.getLegacyPreKeyStore() != null) {
            logger.debug("Migrating legacy pre key store.");
            for (var entry : legacySignalProtocolStore.getLegacyPreKeyStore().getPreKeys().entrySet()) {
                try {
                    preKeyStore.storePreKey(entry.getKey(), new PreKeyRecord(entry.getValue()));
                } catch (IOException e) {
                    logger.warn("Failed to migrate pre key, ignoring", e);
                }
            }
            migrated = true;
        }

        if (legacySignalProtocolStore != null && legacySignalProtocolStore.getLegacySignedPreKeyStore() != null) {
            logger.debug("Migrating legacy signed pre key store.");
            for (var entry : legacySignalProtocolStore.getLegacySignedPreKeyStore().getSignedPreKeys().entrySet()) {
                try {
                    signedPreKeyStore.storeSignedPreKey(entry.getKey(), new SignedPreKeyRecord(entry.getValue()));
                } catch (IOException e) {
                    logger.warn("Failed to migrate signed pre key, ignoring", e);
                }
            }
            migrated = true;
        }

        if (legacySignalProtocolStore != null && legacySignalProtocolStore.getLegacySessionStore() != null) {
            logger.debug("Migrating legacy session store.");
            for (var session : legacySignalProtocolStore.getLegacySessionStore().getSessions()) {
                try {
                    sessionStore.storeSession(new SignalProtocolAddress(session.address.getIdentifier(),
                            session.deviceId), new SessionRecord(session.sessionRecord));
                } catch (IOException e) {
                    logger.warn("Failed to migrate session, ignoring", e);
                }
            }
            migrated = true;
        }

        if (legacySignalProtocolStore != null && legacySignalProtocolStore.getLegacyIdentityKeyStore() != null) {
            logger.debug("Migrating legacy identity session store.");
            for (var identity : legacySignalProtocolStore.getLegacyIdentityKeyStore().getIdentities()) {
                RecipientId recipientId = recipientStore.resolveRecipientTrusted(identity.getAddress());
                identityKeyStore.saveIdentity(recipientId, identity.getIdentityKey(), identity.getDateAdded());
                identityKeyStore.setIdentityTrustLevel(recipientId,
                        identity.getIdentityKey(),
                        identity.getTrustLevel());
            }
            migrated = true;
        }

        if (rootNode.hasNonNull("contactStore")) {
            logger.debug("Migrating legacy contact store.");
            final var contactStoreNode = rootNode.get("contactStore");
            final var contactStore = jsonProcessor.convertValue(contactStoreNode, LegacyJsonContactsStore.class);
            for (var contact : contactStore.getContacts()) {
                final var recipientId = recipientStore.resolveRecipientTrusted(contact.getAddress());
                recipientStore.storeContact(recipientId,
                        new Contact(contact.name,
                                contact.color,
                                contact.messageExpirationTime,
                                contact.blocked,
                                contact.archived));

                // Store profile keys only in profile store
                var profileKeyString = contact.profileKey;
                if (profileKeyString != null) {
                    final ProfileKey profileKey;
                    try {
                        profileKey = new ProfileKey(Base64.getDecoder().decode(profileKeyString));
                        getProfileStore().storeProfileKey(recipientId, profileKey);
                    } catch (InvalidInputException e) {
                        logger.warn("Failed to parse legacy contact profile key: {}", e.getMessage());
                    }
                }
            }
            migrated = true;
        }

        if (rootNode.hasNonNull("profileStore")) {
            logger.debug("Migrating legacy profile store.");
            var profileStoreNode = rootNode.get("profileStore");
            final var legacyProfileStore = jsonProcessor.convertValue(profileStoreNode, LegacyProfileStore.class);
            for (var profileEntry : legacyProfileStore.getProfileEntries()) {
                var recipientId = recipientStore.resolveRecipient(profileEntry.getAddress());
                recipientStore.storeProfileKeyCredential(recipientId, profileEntry.getProfileKeyCredential());
                recipientStore.storeProfileKey(recipientId, profileEntry.getProfileKey());
                final var profile = profileEntry.getProfile();
                if (profile != null) {
                    final var capabilities = new HashSet<Profile.Capability>();
                    if (profile.getCapabilities() != null) {
                        if (profile.getCapabilities().gv1Migration) {
                            capabilities.add(Profile.Capability.gv1Migration);
                        }
                        if (profile.getCapabilities().gv2) {
                            capabilities.add(Profile.Capability.gv2);
                        }
                        if (profile.getCapabilities().storage) {
                            capabilities.add(Profile.Capability.storage);
                        }
                    }
                    final var newProfile = new Profile(profileEntry.getLastUpdateTimestamp(),
                            profile.getGivenName(),
                            profile.getFamilyName(),
                            profile.getAbout(),
                            profile.getAboutEmoji(),
                            profile.isUnrestrictedUnidentifiedAccess()
                                    ? Profile.UnidentifiedAccessMode.UNRESTRICTED
                                    : profile.getUnidentifiedAccess() != null
                                            ? Profile.UnidentifiedAccessMode.ENABLED
                                            : Profile.UnidentifiedAccessMode.DISABLED,
                            capabilities);
                    recipientStore.storeProfile(recipientId, newProfile);
                }
            }
        }

        return migrated;
    }

    private boolean loadLegacyThreadStore(final JsonNode rootNode) {
        var threadStoreNode = rootNode.get("threadStore");
        if (threadStoreNode != null && !threadStoreNode.isNull()) {
            var threadStore = jsonProcessor.convertValue(threadStoreNode, LegacyJsonThreadStore.class);
            // Migrate thread info to group and contact store
            for (var thread : threadStore.getThreads()) {
                if (thread.id == null || thread.id.isEmpty()) {
                    continue;
                }
                try {
                    if (UuidUtil.isUuid(thread.id) || thread.id.startsWith("+")) {
                        final var recipientId = recipientStore.resolveRecipient(thread.id);
                        var contact = recipientStore.getContact(recipientId);
                        if (contact != null) {
                            recipientStore.storeContact(recipientId,
                                    Contact.newBuilder(contact)
                                            .withMessageExpirationTime(thread.messageExpirationTime)
                                            .build());
                        }
                    } else {
                        var groupInfo = groupStore.getGroup(GroupId.fromBase64(thread.id));
                        if (groupInfo instanceof GroupInfoV1) {
                            ((GroupInfoV1) groupInfo).messageExpirationTime = thread.messageExpirationTime;
                            groupStore.updateGroup(groupInfo);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to read legacy thread info: {}", e.getMessage());
                }
            }
            return true;
        }

        return false;
    }

    private void saveStickerStore(StickerStore.Storage storage) {
        this.stickerStoreStorage = storage;
        save();
    }

    private void saveGroupStore(GroupStore.Storage storage) {
        this.groupStoreStorage = storage;
        save();
    }

    private void save() {
        synchronized (fileChannel) {
            var rootNode = jsonProcessor.createObjectNode();
            rootNode.put("version", CURRENT_STORAGE_VERSION)
                    .put("username", username)
                    .put("uuid", uuid == null ? null : uuid.toString())
                    .put("deviceName", encryptedDeviceName)
                    .put("deviceId", deviceId)
                    .put("isMultiDevice", isMultiDevice)
                    .put("lastReceiveTimestamp", lastReceiveTimestamp)
                    .put("password", password)
                    .put("registrationId", identityKeyStore.getLocalRegistrationId())
                    .put("identityPrivateKey",
                            Base64.getEncoder()
                                    .encodeToString(identityKeyStore.getIdentityKeyPair().getPrivateKey().serialize()))
                    .put("identityKey",
                            Base64.getEncoder()
                                    .encodeToString(identityKeyStore.getIdentityKeyPair().getPublicKey().serialize()))
                    .put("registrationLockPin", registrationLockPin)
                    .put("pinMasterKey",
                            pinMasterKey == null ? null : Base64.getEncoder().encodeToString(pinMasterKey.serialize()))
                    .put("storageKey",
                            storageKey == null ? null : Base64.getEncoder().encodeToString(storageKey.serialize()))
                    .put("storageManifestVersion", storageManifestVersion == -1 ? null : storageManifestVersion)
                    .put("preKeyIdOffset", preKeyIdOffset)
                    .put("nextSignedPreKeyId", nextSignedPreKeyId)
                    .put("profileKey",
                            profileKey == null ? null : Base64.getEncoder().encodeToString(profileKey.serialize()))
                    .put("registered", registered)
                    .putPOJO("groupStore", groupStoreStorage)
                    .putPOJO("stickerStore", stickerStoreStorage);
            try {
                // Write to memory first to prevent corrupting the file in case of serialization errors
                var input = new ByteArrayInputStream(jsonProcessor.writeValueAsBytes(rootNode));
                fileChannel.position(0);
                try (OutputStream out = createOutputStream()) {
                	input.transferTo(out);
                }	
                fileChannel.truncate(fileChannel.position());
                fileChannel.force(false);
            } catch (Exception e) {
                logger.error("Error saving file: {}", e.getMessage());
            }
        }
    }
    
    private OutputStream createOutputStream() throws IOException {
    	OutputStream out = NonCloseableOutputStream.create(fileChannel);
    	return cipherStreamFactory != null
    			? cipherStreamFactory.createOutputStream(username, out)
    			: out;
    }
    
    private InputStream createInputStream() throws IOException {
    	InputStream in = Channels.newInputStream(fileChannel);
    	return cipherStreamFactory != null ? cipherStreamFactory.createInputStream(username, in) : in;
    }

    private static Pair<FileChannel, FileLock> openFileChannel(File fileName, boolean waitForLock) throws IOException {
        var fileChannel = new RandomAccessFile(fileName, "rw").getChannel();
        var lock = fileChannel.tryLock();
        if (lock == null) {
            if (!waitForLock) {
                logger.debug("Config file is in use by another instance.");
                throw new IOException("Config file is in use by another instance.");
            }
            logger.info("Config file is in use by another instance, waiting…");
            lock = fileChannel.lock();
            logger.info("Config file lock acquired.");
        }
        return new Pair<>(fileChannel, lock);
    }

    public void addPreKeys(List<PreKeyRecord> records) {
        for (var record : records) {
            if (preKeyIdOffset != record.getId()) {
                logger.error("Invalid pre key id {}, expected {}", record.getId(), preKeyIdOffset);
                throw new AssertionError("Invalid pre key id");
            }
            preKeyStore.storePreKey(record.getId(), record);
            preKeyIdOffset = (preKeyIdOffset + 1) % Medium.MAX_VALUE;
        }
        save();
    }

    public void addSignedPreKey(SignedPreKeyRecord record) {
        if (nextSignedPreKeyId != record.getId()) {
            logger.error("Invalid signed pre key id {}, expected {}", record.getId(), nextSignedPreKeyId);
            throw new AssertionError("Invalid signed pre key id");
        }
        signalProtocolStore.storeSignedPreKey(record.getId(), record);
        nextSignedPreKeyId = (nextSignedPreKeyId + 1) % Medium.MAX_VALUE;
        save();
    }

    public SignalProtocolStore getSignalProtocolStore() {
        return signalProtocolStore;
    }

    public SessionStore getSessionStore() {
        return sessionStore;
    }

    public IdentityKeyStore getIdentityKeyStore() {
        return identityKeyStore;
    }

    public GroupStore getGroupStore() {
        return groupStore;
    }

    public ContactsStore getContactStore() {
        return recipientStore;
    }

    public RecipientStore getRecipientStore() {
        return recipientStore;
    }

    public ProfileStore getProfileStore() {
        return recipientStore;
    }

    public StickerStore getStickerStore() {
        return stickerStore;
    }

    public SenderKeyStore getSenderKeyStore() {
        return senderKeyStore;
    }

    public MessageCache getMessageCache() {
        return messageCache;
    }

    public String getUsername() {
        return username;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(final UUID uuid) {
        this.uuid = uuid;
        save();
    }

    public SignalServiceAddress getSelfAddress() {
        return new SignalServiceAddress(uuid, username);
    }

    public RecipientId getSelfRecipientId() {
        return recipientStore.resolveRecipientTrusted(new RecipientAddress(uuid, username));
    }

    public String getEncryptedDeviceName() {
        return encryptedDeviceName;
    }

    public void setEncryptedDeviceName(final String encryptedDeviceName) {
        this.encryptedDeviceName = encryptedDeviceName;
        save();
    }

    public int getDeviceId() {
        return deviceId;
    }

    public boolean isMasterDevice() {
        return deviceId == SignalServiceAddress.DEFAULT_DEVICE_ID;
    }

    public IdentityKeyPair getIdentityKeyPair() {
        return signalProtocolStore.getIdentityKeyPair();
    }

    public int getLocalRegistrationId() {
        return signalProtocolStore.getLocalRegistrationId();
    }

    public String getPassword() {
        return password;
    }

    private void setPassword(final String password) {
        this.password = password;
        save();
    }

    public String getRegistrationLockPin() {
        return registrationLockPin;
    }

    public void setRegistrationLockPin(final String registrationLockPin, final MasterKey pinMasterKey) {
        this.registrationLockPin = registrationLockPin;
        this.pinMasterKey = pinMasterKey;
        save();
    }

    public MasterKey getPinMasterKey() {
        return pinMasterKey;
    }

    public StorageKey getStorageKey() {
        if (pinMasterKey != null) {
            return pinMasterKey.deriveStorageServiceKey();
        }
        return storageKey;
    }

    public void setStorageKey(final StorageKey storageKey) {
        if (storageKey.equals(this.storageKey)) {
            return;
        }
        this.storageKey = storageKey;
        save();
    }

    public long getStorageManifestVersion() {
        return this.storageManifestVersion;
    }

    public void setStorageManifestVersion(final long storageManifestVersion) {
        if (storageManifestVersion == this.storageManifestVersion) {
            return;
        }
        this.storageManifestVersion = storageManifestVersion;
        save();
    }

    public ProfileKey getProfileKey() {
        return profileKey;
    }

    public void setProfileKey(final ProfileKey profileKey) {
        if (profileKey.equals(this.profileKey)) {
            return;
        }
        this.profileKey = profileKey;
        save();
    }

    public byte[] getSelfUnidentifiedAccessKey() {
        return UnidentifiedAccess.deriveAccessKeyFrom(getProfileKey());
    }

    public int getPreKeyIdOffset() {
        return preKeyIdOffset;
    }

    public int getNextSignedPreKeyId() {
        return nextSignedPreKeyId;
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(final boolean registered) {
        this.registered = registered;
        save();
    }

    public boolean isMultiDevice() {
        return isMultiDevice;
    }

    public void setMultiDevice(final boolean multiDevice) {
        if (isMultiDevice == multiDevice) {
            return;
        }
        isMultiDevice = multiDevice;
        save();
    }

    public long getLastReceiveTimestamp() {
        return lastReceiveTimestamp;
    }

    public void setLastReceiveTimestamp(final long lastReceiveTimestamp) {
        this.lastReceiveTimestamp = lastReceiveTimestamp;
        save();
    }

    public boolean isUnrestrictedUnidentifiedAccess() {
        // TODO make configurable
        return false;
    }

    public boolean isDiscoverableByPhoneNumber() {
        // TODO make configurable
        return true;
    }

    public boolean isPhoneNumberShared() {
        // TODO make configurable
        return true;
    }

    public void finishRegistration(final UUID uuid, final MasterKey masterKey, final String pin) {
        this.pinMasterKey = masterKey;
        this.storageManifestVersion = -1;
        this.storageKey = null;
        this.encryptedDeviceName = null;
        this.deviceId = SignalServiceAddress.DEFAULT_DEVICE_ID;
        this.isMultiDevice = false;
        this.registered = true;
        this.uuid = uuid;
        this.registrationLockPin = pin;
        this.lastReceiveTimestamp = 0;
        save();

        getSessionStore().archiveAllSessions();
        senderKeyStore.deleteAll();
        final var recipientId = getRecipientStore().resolveRecipientTrusted(getSelfAddress());
        final var publicKey = getIdentityKeyPair().getPublicKey();
        getIdentityKeyStore().saveIdentity(recipientId, publicKey, new Date());
        getIdentityKeyStore().setIdentityTrustLevel(recipientId, publicKey, TrustLevel.TRUSTED_VERIFIED);
    }

    @Override
    public void close() throws IOException {
    	identityKeyStore.close();
    	sessionStore.close();
    	signedPreKeyStore.close();
    	preKeyStore.close();
    	recipientStore.close();
    	
        synchronized (fileChannel) {
            try {
                lock.close();
            } catch (ClosedChannelException ignored) {
            }
            fileChannel.close();
        }
    }
}
