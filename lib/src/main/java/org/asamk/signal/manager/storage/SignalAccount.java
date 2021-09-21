package org.asamk.signal.manager.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.contacts.ContactsStore;
import org.asamk.signal.manager.storage.groups.IGroupStore;
import org.asamk.signal.manager.storage.identities.IIdentityKeyStore;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.manager.storage.messageCache.MessageCache;
import org.asamk.signal.manager.storage.prekeys.IPreKeyStore;
import org.asamk.signal.manager.storage.prekeys.ISignedPreKeyStore;
import org.asamk.signal.manager.storage.profiles.ProfileStore;
import org.asamk.signal.manager.storage.protocol.SignalProtocolStore;
import org.asamk.signal.manager.storage.recipients.IRecipientStore;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientMergeHandler;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.asamk.signal.manager.storage.senderKeys.ISenderKeyStore;
import org.asamk.signal.manager.storage.sessions.ISessionStore;
import org.asamk.signal.manager.storage.stickers.IStickerStore;
import org.asamk.signal.manager.util.IOUtils;
import org.asamk.signal.manager.util.KeyUtils;
import org.hsqldb.jdbc.JDBCPool;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKey;
import org.whispersystems.libsignal.IdentityKeyPair;
import org.whispersystems.libsignal.state.PreKeyRecord;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;
import org.whispersystems.signalservice.api.crypto.UnidentifiedAccess;
import org.whispersystems.signalservice.api.kbs.MasterKey;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;
import org.whispersystems.signalservice.api.storage.StorageKey;

import io.kryptoworx.signalcli.storage.HsqlAccountStore;
import io.kryptoworx.signalcli.storage.HsqlGroupStore;
import io.kryptoworx.signalcli.storage.HsqlIdentityKeyStore;
import io.kryptoworx.signalcli.storage.HsqlPreKeyStore;
import io.kryptoworx.signalcli.storage.HsqlRecipientStore;
import io.kryptoworx.signalcli.storage.HsqlSenderKeyStore;
import io.kryptoworx.signalcli.storage.HsqlSessionStore;
import io.kryptoworx.signalcli.storage.HsqlSignedPreKeyStore;
import io.kryptoworx.signalcli.storage.HsqlStickerStore;
import io.kryptoworx.signalcli.storage.SQLConnectionFactory;

public class SignalAccount implements Closeable {

    public static final int MINIMUM_STORAGE_VERSION = 1;
    public static final int CURRENT_STORAGE_VERSION = 2;

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
    private long lastReceiveTimestamp = 0;

    private boolean registered = false;

    private SignalProtocolStore signalProtocolStore;
    private IPreKeyStore preKeyStore;
    private ISignedPreKeyStore signedPreKeyStore;
    private ISessionStore sessionStore;
    private IIdentityKeyStore identityKeyStore;
    private ISenderKeyStore senderKeyStore;
    private IGroupStore groupStore;
    private IRecipientStore recipientStore;
    private IStickerStore stickerStore;

    private MessageCache messageCache;
    
    private final HsqlAccountStore accountStore;
    
    public class Builder {
        private int registrationId = 0;
        private IdentityKeyPair identityKeyPair;
        
        private Builder() { }
        
        public Builder uuid(UUID uuid) {
            SignalAccount.this.uuid = uuid;
            return this;
        }
        
        public Builder username(String username) {
            SignalAccount.this.username = username;
            return this;
        }
        
        public Builder deviceId(int deviceId) {
            SignalAccount.this.deviceId = deviceId;
            return this;
        }

        public Builder deviceName(String encryptedDeviceName) {
            SignalAccount.this.encryptedDeviceName = encryptedDeviceName;
            return this;
        }
        
        public Builder multiDevice(boolean multiDevice) {
            SignalAccount.this.isMultiDevice = multiDevice;
            return this;
        }
        
        public Builder lastReceiveTimestamp(long lastReceiveTimestamp) {
            SignalAccount.this.lastReceiveTimestamp = lastReceiveTimestamp;
            return this;
        }
        
        public Builder password(String password) {
            SignalAccount.this.password = password;
            return this;
        }
        
        public Builder registrationId(int registrationId) {
            this.registrationId = registrationId;
            return this;
        }
        
        public Builder identityKey(byte[] privateKeyBytes, byte[] publicKeyBytes) {
            if (privateKeyBytes != null && publicKeyBytes != null) {
                identityKeyPair = KeyUtils.getIdentityKeyPair(publicKeyBytes, privateKeyBytes);
            }
            return this;
        }
        
        public Builder profileKey(byte[] keyBytes) {
            SignalAccount.this.profileKey = keyBytes != null ? newProfileKey(keyBytes) : null;
            return this;
        }
        
        private static ProfileKey newProfileKey(byte[] keyBytes) {
            try {
                return new ProfileKey(keyBytes);
            } catch (InvalidInputException e) {
                throw new AssertionError();
            }
        }
        
        public Builder registered(boolean registered) {
            SignalAccount.this.registered = registered;
            return this;
        }
        
        public Builder registrationLockPin(String registrationLockPin) {
            SignalAccount.this.registrationLockPin = registrationLockPin;
            return this;
        }
        
        public Builder pinMasterKey(byte[] keyBytes) {
            SignalAccount.this.pinMasterKey = keyBytes != null ? new MasterKey(keyBytes) : null;
            return this;
        }
        
        public Builder storageKey(byte[] keyBytes) {
            SignalAccount.this.storageKey = keyBytes != null ? new StorageKey(keyBytes) : null;
            return this;
        }
        
        public Builder storageManifestVersion(long storageManifestVersion) {
            SignalAccount.this.storageManifestVersion = storageManifestVersion;
            return this;
        }
        
        public SignalAccount build() {
            return SignalAccount.this;
        }
    }

    private Builder createBuilder() {
        return new Builder();
    }
    
    private SignalAccount(HsqlAccountStore accountStore) {
        this.accountStore = accountStore;
    }
    
    public static SignalAccount load(HsqlAccountStore accountStore, File dataPath, String username, boolean waitForLock, TrustNewIdentity trustNewIdentity) throws IOException {
        var account = new SignalAccount(accountStore);
        account.username = username;
        if (!account.load(dataPath, trustNewIdentity)) {
            return null;
        }
        if (!username.equals(account.getUsername())) {
            throw new IOException("Username in account file doesn't match expected number: "
                    + account.getUsername());
        }
        return account;
    }
    
    public static HsqlAccountStore createAccountStore(File dataPath) throws IOException {
        IOUtils.createPrivateDirectories(dataPath);
        String databaseUrl = String.format("jdbc:hsqldb:file:%s", new File(dataPath, "db"));
        JDBCPool connectionPool = new JDBCPool(4);
        connectionPool.setURL(databaseUrl);
        return new HsqlAccountStore(connectionPool::getConnection);
   }

    public static SignalAccount create(
            File dataPath,
            HsqlAccountStore accountStore,
            String username,
            IdentityKeyPair identityKey,
            int registrationId,
            ProfileKey profileKey,
            final TrustNewIdentity trustNewIdentity
    ) throws IOException {
        var account = new SignalAccount(accountStore);
        account.username = username;
        account.profileKey = profileKey;
        account.initStores(dataPath, identityKey, registrationId, trustNewIdentity);
        account.registered = false;
        account.migrateLegacyConfigs();
        account.save();
        return account;
    }

    private void initStores(
            final File dataPath,
            final IdentityKeyPair identityKey,
            final int registrationId,
            final TrustNewIdentity trustNewIdentity
    ) throws IOException {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            
            RecipientMergeHandler recipientMergeHandler = new RecipientMergeHandler() {
                @Override
                public void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) {
                    SignalAccount.this.mergeRecipients(recipientId, toBeMergedRecipientId);
                }
                
                @Override
                public void beforeMergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
                    sessionStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
                    groupStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
                    senderKeyStore.mergeRecipients(connection, recipientId, toBeMergedRecipientId);
                }
            };
            
            recipientStore = createRecipientStore(dataPath, accountStore, recipientMergeHandler);
            preKeyStore = createPreKeyStore(dataPath, accountStore);
            signedPreKeyStore = createSignedPreKeyStore(dataPath, accountStore);
            identityKeyStore = createIdentityKeyStore(dataPath, accountStore, 
                    identityKey, registrationId, trustNewIdentity);
            sessionStore = createSessionStore(dataPath, accountStore, recipientStore);
            senderKeyStore = createSenderKeyStore(dataPath, accountStore, recipientStore);
            
            groupStore = new HsqlGroupStore(accountStore, username, recipientStore);
            stickerStore = new HsqlStickerStore(accountStore);
            
            signalProtocolStore = new SignalProtocolStore(preKeyStore,
                    signedPreKeyStore,
                    sessionStore,
                    identityKeyStore,
                    senderKeyStore,
                    this::isMultiDevice);
            messageCache = new MessageCache(getMessageCachePath(dataPath, username));
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load database driver");
        }
    }

    private IRecipientStore createRecipientStore(File dataPath, 
            SQLConnectionFactory connectionFactory, 
            RecipientMergeHandler recipientMergeHandler) throws IOException {
        return new HsqlRecipientStore(connectionFactory, recipientMergeHandler);
    }
    
    private IPreKeyStore createPreKeyStore(File dataPath, SQLConnectionFactory connectionFactory) throws IOException {
        return new HsqlPreKeyStore(connectionFactory);
    }

    private ISignedPreKeyStore createSignedPreKeyStore(File dataPath, SQLConnectionFactory connectionFactory) throws IOException {
        return new HsqlSignedPreKeyStore(connectionFactory);
    }
    
    private IIdentityKeyStore createIdentityKeyStore(File dataPath, 
            SQLConnectionFactory connectionFactory,
            IdentityKeyPair identityKey,
            int registrationId,
            TrustNewIdentity trustNewIdentity) throws IOException {
        return new HsqlIdentityKeyStore(connectionFactory,
                recipientStore,
                identityKey,
                registrationId,
                trustNewIdentity);
    }
    
    private ISessionStore createSessionStore(File dataPath, 
            SQLConnectionFactory connectionFactory, 
            RecipientResolver recipientResolver) throws IOException {
        return new HsqlSessionStore(connectionFactory, recipientResolver);
    }
    
    private ISenderKeyStore createSenderKeyStore(File dataPath,
            SQLConnectionFactory connectionFactory,
            IRecipientStore recipientStore) throws IOException {
        return new HsqlSenderKeyStore(connectionFactory, recipientStore);
    }
    
    public static SignalAccount createOrUpdateLinkedAccount(
            HsqlAccountStore accountStore,
            File dataPath,
            String username,
            UUID uuid,
            String password,
            String encryptedDeviceName,
            int deviceId,
            IdentityKeyPair identityKey,
            int registrationId,
            ProfileKey profileKey,
            final TrustNewIdentity trustNewIdentity
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
                    trustNewIdentity);
        }

        final var account = load(accountStore, dataPath, username, true, trustNewIdentity);
        account.setProvisioningData(username, uuid, password, encryptedDeviceName, deviceId, profileKey);
        account.recipientStore.resolveRecipientTrusted(account.getSelfAddress());
        account.sessionStore.archiveAllSessions();
        account.senderKeyStore.deleteAll();
        account.clearAllPreKeys();
        return account;
    }

    private void clearAllPreKeys() {
        this.preKeyStore.removeAllPreKeys();
        this.signedPreKeyStore.removeAllSignedPreKeys();
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
            final TrustNewIdentity trustNewIdentity
    ) throws IOException {
        var fileName = getFileName(dataPath, username);
        IOUtils.createPrivateFile(fileName);
        var account = new SignalAccount(createAccountStore(dataPath));
        account.setProvisioningData(username, uuid, password, encryptedDeviceName, deviceId, profileKey);
        account.initStores(dataPath, identityKey, registrationId, trustNewIdentity);
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

    private boolean load(File dataPath, final TrustNewIdentity trustNewIdentity) throws IOException {
        Builder builder = createBuilder();
        if (!accountStore.loadAccount(username, builder)) {
            return false;
        }
        initStores(dataPath, builder.identityKeyPair, builder.registrationId, trustNewIdentity);
        return true;
    }

    private void save() {
        accountStore.storeAccount(this);
    }

    public void addPreKeys(List<PreKeyRecord> records) {
        for (var record : records) {
            preKeyStore.storePreKey(record.getId(), record);
        }
    }

    public void addSignedPreKey(SignedPreKeyRecord record) {
        signalProtocolStore.storeSignedPreKey(record.getId(), record);
    }

    public SignalProtocolStore getSignalProtocolStore() {
        return signalProtocolStore;
    }

    public ISessionStore getSessionStore() {
        return sessionStore;
    }

    public IIdentityKeyStore getIdentityKeyStore() {
        return identityKeyStore;
    }

    public IGroupStore getGroupStore() {
        return groupStore;
    }

    public ContactsStore getContactStore() {
        return recipientStore;
    }

    public IRecipientStore getRecipientStore() {
        return recipientStore;
    }

    public ProfileStore getProfileStore() {
        return recipientStore;
    }

    public IStickerStore getStickerStore() {
        return stickerStore;
    }

    public ISenderKeyStore getSenderKeyStore() {
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
        if (Objects.equals(this.uuid, uuid)) {
            return;
        }
        this.uuid = uuid;
        accountStore.updateId(username, uuid);
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
        accountStore.updateDeviceName(username, encryptedDeviceName);
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
        if (Objects.equals(this.password, password)) {
            return;
        }
        this.password = password;
        accountStore.updatePassword(username, password);
    }

    public String getRegistrationLockPin() {
        return registrationLockPin;
    }

    public void setRegistrationLockPin(final String registrationLockPin, final MasterKey pinMasterKey) {
        this.registrationLockPin = registrationLockPin;
        this.pinMasterKey = pinMasterKey;
        accountStore.updateRegistrationLockPin(username, registrationLockPin, pinMasterKey);
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
        accountStore.updateStorageManifestVersion(username, storageManifestVersion);
    }

    public long getStorageManifestVersion() {
        return this.storageManifestVersion;
    }

    public void setStorageManifestVersion(final long storageManifestVersion) {
        if (storageManifestVersion == this.storageManifestVersion) {
            return;
        }
        this.storageManifestVersion = storageManifestVersion;
        accountStore.updateStorageManifestVersion(username, storageManifestVersion);
    }

    public ProfileKey getProfileKey() {
        return profileKey;
    }

    public void setProfileKey(final ProfileKey profileKey) {
        if (profileKey.equals(this.profileKey)) {
            return;
        }
        this.profileKey = profileKey;
        accountStore.updateProfileKey(username, profileKey);
    }

    public byte[] getSelfUnidentifiedAccessKey() {
        return UnidentifiedAccess.deriveAccessKeyFrom(getProfileKey());
    }

    public int getPreKeyIdOffset() {
        return preKeyStore.getPreKeyIdOffset();
    }

    public int getNextSignedPreKeyId() {
        return signedPreKeyStore.getNextSignedPreKeyId();
    }

    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(final boolean registered) {
        if (this.registered != registered) {
            this.registered = registered;
            accountStore.updateRegistered(username, registered);
        }
    }

    public boolean isMultiDevice() {
        return isMultiDevice;
    }

    public void setMultiDevice(final boolean multiDevice) {
        if (isMultiDevice != multiDevice) {
            this.isMultiDevice = multiDevice;
            accountStore.updateMultiDevice(username, multiDevice);
        }
    }

    public long getLastReceiveTimestamp() {
        return lastReceiveTimestamp;
    }

    public void setLastReceiveTimestamp(final long lastReceiveTimestamp) {
        this.lastReceiveTimestamp = lastReceiveTimestamp;
        accountStore.updateLastReceiveTimestamp(username, lastReceiveTimestamp);
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
        
        // TODO the following store action should be run atomically, i.e. in the same transaction
        
        accountStore.finishRegistration(this);

        getSessionStore().archiveAllSessions();
        senderKeyStore.deleteAll();
        final var recipientId = getRecipientStore().resolveRecipientTrusted(getSelfAddress());
        final var publicKey = getIdentityKeyPair().getPublicKey();
        getIdentityKeyStore().saveIdentity(recipientId, publicKey, new Date());
        getIdentityKeyStore().setIdentityTrustLevel(recipientId, publicKey, TrustLevel.TRUSTED_VERIFIED);
    }

    @Override
    public void close() throws IOException {
        try {
            accountStore.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close account store", e);
        }
    }
}
