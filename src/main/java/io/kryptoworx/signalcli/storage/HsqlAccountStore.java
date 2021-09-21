package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.asamk.signal.manager.storage.SignalAccount;
import org.signal.zkgroup.profiles.ProfileKey;
import org.whispersystems.signalservice.api.kbs.MasterKey;
import org.whispersystems.signalservice.api.storage.StorageKey;

public class HsqlAccountStore extends HsqlStore implements SQLConnectionFactory, AutoCloseable {
    public HsqlAccountStore(SQLConnectionFactory connectionFactory) {
        super(connectionFactory);
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS account
                (
                    id UUID,
                    version INT NOT NULL,
                    e164 VARCHAR(20 CHARACTERS) NOT NULL PRIMARY KEY,
                    device_id INT NOT NULL,
                    device_name VARCHAR(100 CHARACTERS),
                    multi_device BOOLEAN NOT NULL,
                    last_receive BIGINT,
                    password VARCHAR(30 CHARACTERS) NOT NULL,
                    registration_id INT NOT NULL,
                    identity_key_private BINARY(32) NOT NULL,
                    identity_key_public BINARY(33) NOT NULL,
                    registration_lock_pin VARCHAR(20 CHARACTERS),
                    pin_master_key BINARY(32),
                    storage_key BINARY(32),
                    storage_manifest_version BIGINT,
                    profile_key BINARY(32) NOT NULL,
                    registered BOOLEAN NOT NULL
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
    }
    
    public void storeAccount(SignalAccount account) {
        voidTransaction(c -> dbStoreAccount(c, account));
    }
    
    private void dbStoreAccount(Connection connection, SignalAccount account) throws SQLException {
        SQLInsert insertAccount = SQLInsert.forTable("account")
                .add("id", account.getUuid(), PreparedStatement::setObject)
                .add("version", SignalAccount.CURRENT_STORAGE_VERSION, PreparedStatement::setInt)
                .add("e164", account.getUsername(), PreparedStatement::setString)
                .add("device_id", account.getDeviceId(), PreparedStatement::setInt)
                .addIfNotNull("device_name", account.getEncryptedDeviceName(), PreparedStatement::setString)
                .add("multi_device", account.isMultiDevice(), PreparedStatement::setBoolean)
                .add("last_receive", account.getLastReceiveTimestamp(), PreparedStatement::setLong)
                .add("password", account.getPassword(), PreparedStatement::setString)
                .add("registration_id", account.getLocalRegistrationId(), PreparedStatement::setInt)
                .add("identity_key_private", 
                        account.getIdentityKeyPair().getPrivateKey().serialize(),
                        PreparedStatement::setBytes)
                .add("identity_key_public",
                        account.getIdentityKeyPair().getPublicKey().serialize(),
                        PreparedStatement::setBytes)
                .addIfNotNull("registration_lock_pin", account.getRegistrationLockPin(), PreparedStatement::setString)
                .add("registered", account.isRegistered(), PreparedStatement::setBoolean)
                ;
        if (account.getPinMasterKey() != null) {
            insertAccount.add("pin_master_key", 
                    account.getPinMasterKey().serialize(), 
                    PreparedStatement::setBytes);
        }
        if (account.getStorageKey() != null) {
            insertAccount.add("storage_key", 
                    account.getStorageKey().serialize(), 
                    PreparedStatement::setBytes);
        }
        if (account.getStorageManifestVersion() >= 0) {
            insertAccount.add("storage_manifest_version",
                    account.getStorageManifestVersion(),
                    PreparedStatement::setLong);
        }
        if (account.getProfileKey() != null) {
            insertAccount.add("profile_key", 
                    account.getProfileKey().serialize(), 
                    PreparedStatement::setBytes);
        }
        insertAccount.execute(connection);
    }
    
    public boolean loadAccount(String username, SignalAccount.Builder accountBuilder) {
        return transaction(c -> dbLoadAccount(c, username, accountBuilder));
    }
    
    private boolean dbLoadAccount(Connection connection, String username, SignalAccount.Builder accountBuilder) throws SQLException {
        String sqlQuery = """
                SELECT 
                    version,
                    id,
                    device_id,
                    multi_device,
                    last_receive,
                    password,
                    registration_id,
                    identity_key_private,
                    identity_key_public,
                    registered,
                    device_name,
                    profile_key,
                    registration_lock_pin,
                    pin_master_key,
                    storage_key,
                    storage_manifest_version
                FROM account
                WHERE e164 = ?
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return false;
                int c = 1;
                int accountVersion = rs.getInt(c++);
                if (accountVersion > SignalAccount.CURRENT_STORAGE_VERSION) {
                    throw new RuntimeException("Account was created by a more recent version!");
                } else if (accountVersion < SignalAccount.MINIMUM_STORAGE_VERSION) {
                    throw new RuntimeException("Account was created by a version no longer supported!");
                }
                if (rs.getObject(c++) instanceof UUID uuid) {
                    accountBuilder.uuid(uuid);
                }
                accountBuilder
                    .username(username)
                    .deviceId(rs.getInt(c++))
                    .multiDevice(rs.getBoolean(c++))
                    .lastReceiveTimestamp(rs.getLong(c++))
                    .password(rs.getString(c++))
                    .registrationId(rs.getInt(c++))
                    .identityKey(rs.getBytes(c++), rs.getBytes(c++))
                    .registered(rs.getBoolean(c++))
                    .deviceName(rs.getString(c++))
                    .profileKey(rs.getBytes(c++))
                    .registrationLockPin(rs.getString(c++))
                    .pinMasterKey(rs.getBytes(c++))
                    .storageKey(rs.getBytes(c++));
                long storageManifestVersion = rs.getLong(c++);
                if (!rs.wasNull()) accountBuilder.storageManifestVersion(storageManifestVersion);
                return true;
            }
        }
    }
    
    public void finishRegistration(SignalAccount account) {
        voidTransaction(c -> dbFinishRegistration(c, account));
    }
    
    private void dbFinishRegistration(Connection connection, SignalAccount account) throws SQLException {
        Optional<MasterKey> pinMasterKey = Optional.ofNullable(account.getPinMasterKey());
        Optional<StorageKey> storageKey = Optional.ofNullable(account.getStorageKey());
        SQLUpdate.forTable("account")
                .set("pin_master_key", SQLArgument.of(pinMasterKey.map(MasterKey::serialize).orElse(null)))
                .set("storage_manifest_version", SQLArgument.of(account.getStorageManifestVersion()))
                .set("storage_key", SQLArgument.of(storageKey.map(StorageKey::serialize).orElse(null)))
                .set("device_name", SQLArgument.of(account.getEncryptedDeviceName()))
                .set("device_id", SQLArgument.of(account.getDeviceId()))
                .set("multi_device", SQLArgument.of(account.isMultiDevice()))
                .set("registered", SQLArgument.of(account.isRegistered()))
                .set("id", SQLArgument.of(account.getUuid()))
                .set("registration_lock_pin", SQLArgument.of(account.getRegistrationLockPin()))
                .set("last_receive", SQLArgument.of(account.getLastReceiveTimestamp()))
                .execute(connection);
                ;
    }

    public void updateLastReceiveTimestamp(String e164, long lastReceiveTimestamp) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "last_receive", SQLArgument.of(lastReceiveTimestamp)));
    }
    
    public void updateMultiDevice(String e164, boolean multiDevice) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "multi_device", SQLArgument.of(multiDevice)));
    }
    
    public void updateRegistered(String e164, boolean registered) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "registered", SQLArgument.of(registered)));
    }
    
    public void updateProfileKey(String e164, ProfileKey profileKey) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "profile_key", 
                SQLArgument.of(profileKey != null ? profileKey.serialize() : null)));
    }
    
    public void updateStorageManifestVersion(String e164, long storageManifestVersion) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "storage_manifest_version", SQLArgument.of(storageManifestVersion)));
    }
    
    public void updatePassword(String e164, String password) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "password", SQLArgument.of(password)));
    }

    public void updateDeviceName(String e164, String deviceName) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "device_name", SQLArgument.of(deviceName)));
    }

    public void updateId(String e164, UUID id) {
        voidTransaction(c -> dbUpdateColumn(c, e164, "id", SQLArgument.of(id)));
    }
    
    private <T> void dbUpdateColumn(Connection connection, String e164, String column, SQLArgument<T> value) throws SQLException {
        String sqlUpdate = "UPDATE account SET %s = ? WHERE e164 = ?".formatted(column);
        try (PreparedStatement stmt = connection.prepareStatement(sqlUpdate)) {
            value.set(stmt, 1);
            stmt.setString(2, e164);
            stmt.executeUpdate();
        }
    }

    public void updateRegistrationLockPin(String e164, String registrationLockPin, MasterKey pinMasterKey) {
        voidTransaction(c -> dbUpdateRegistrationLockPin(c, e164, registrationLockPin, pinMasterKey));
    }
    
    private void dbUpdateRegistrationLockPin(Connection connection,
            String e164, String registrationLockPin, MasterKey pinMasterKey) throws SQLException {
        byte[] masterKeyBytes = pinMasterKey != null ? pinMasterKey.serialize() : null;
        String sqlUpdate = "UPDATE account SET registration_lock_pin = ?, pin_master_key = ? WHERE e164 = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlUpdate)) {
            stmt.setString(1, registrationLockPin);
            stmt.setBytes(2, masterKeyBytes);
            stmt.setString(3, e164);
            stmt.executeUpdate();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return super.getConnection();
    }

    public boolean userExists(String username) {
        return transaction(c -> dbUserExists(c, username));
    }
    
    private boolean dbUserExists(Connection connection, String username) throws SQLException {
        String sqlQuery = "SELECT COUNT(*) FROM account WHERE e164 = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
}
