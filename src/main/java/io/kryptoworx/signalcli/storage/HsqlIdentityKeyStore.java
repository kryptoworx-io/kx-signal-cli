package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.identities.IIdentityKeyStore;
import org.asamk.signal.manager.storage.identities.IdentityInfo;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.whispersystems.libsignal.IdentityKey;
import org.whispersystems.libsignal.IdentityKeyPair;
import org.whispersystems.libsignal.InvalidKeyException;
import org.whispersystems.libsignal.SignalProtocolAddress;

public class HsqlIdentityKeyStore extends HsqlStore implements IIdentityKeyStore {

    private final RecipientResolver resolver;
    private final IdentityKeyPair identityKeyPair;
    private final int localRegistrationId;
    private final TrustNewIdentity trustNewIdentity;

    public HsqlIdentityKeyStore(SQLConnectionFactory connectionFactory,
            RecipientResolver resolver,
            IdentityKeyPair identityKeyPair,
            int localRegistrationId,
            TrustNewIdentity trustNewIdentity) {
        super(connectionFactory);
        this.resolver = resolver;
        this.identityKeyPair = identityKeyPair;
        this.localRegistrationId = localRegistrationId;
        this.trustNewIdentity = trustNewIdentity;
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS identity
                (
                    recipient BIGINT PRIMARY KEY NOT NULL 
                        REFERENCES recipient (ID)
                        ON DELETE CASCADE,
                    identity_key VARBINARY(150) NOT NULL,
                    trust_level TINYINT NOT NULL,
                    date_added DATE NOT NULL
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
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

    @Override
    public boolean saveIdentity(RecipientId recipientId, IdentityKey identityKey, Date added) {
        return transaction(c -> dbUpdateIdentity(c, recipientId.getId(), identityKey, added));
    }
    
    private boolean dbUpdateIdentity(Connection connection, long recipientId, IdentityKey identityKey, Date dateAdded) throws SQLException {
        String sqlMergeIdentity = """
                MERGE INTO identity t
                USING (VALUES ?, CAST(? AS VARBINARY(150))) AS s(rcpt, k) 
                ON s.rcpt = t.recipient
                WHEN MATCHED AND s.k <> t.identity_key THEN UPDATE SET t.trust_level = ?
                WHEN NOT MATCHED THEN 
                    INSERT (recipient, identity_key, trust_level, date_added)
                    VALUES (s.rcpt, s.k, ?, ?);
                """;
        
        TrustLevel trustLevelNew = TrustLevel.UNTRUSTED, trustLevelUpdate = TrustLevel.UNTRUSTED;
        switch (trustNewIdentity) {
        case ALWAYS:
            trustLevelNew = trustLevelUpdate = TrustLevel.TRUSTED_UNVERIFIED;
            break;
        case ON_FIRST_USE:
            trustLevelNew = TrustLevel.TRUSTED_UNVERIFIED;
            break;
        case NEVER:
            break;
        }
        
        try (PreparedStatement stmt = connection.prepareStatement(sqlMergeIdentity)) {
            int p = 1;
            stmt.setLong(p++, recipientId);
            stmt.setBytes(p++, identityKey.serialize());
            stmt.setInt(p++, trustLevelUpdate.ordinal());
            stmt.setInt(p++, trustLevelNew.ordinal());
            stmt.setDate(p++, new java.sql.Date(dateAdded.getTime()));
            return stmt.executeUpdate() > 0;
        }
    }

    @Override
    public boolean setIdentityTrustLevel(RecipientId recipientId, IdentityKey identityKey, TrustLevel trustLevel) {
        return transaction(c -> dbUpdateTrustLevel(c, recipientId.getId(), identityKey, trustLevel));
    }
    
    private boolean dbUpdateTrustLevel(Connection connection, long recipientId, IdentityKey identityKey, TrustLevel trustLevel) throws SQLException {
        String sqlUpdateTrustLevel = "UPDATE identity SET trust_level = ? WHERE recipient = ? AND identity_key = ?";
        return transaction(c -> {
            try (PreparedStatement stmt = c.prepareStatement(sqlUpdateTrustLevel)) {
                int p = 1;
                stmt.setInt(p++, trustLevel.ordinal());
                stmt.setLong(p++, recipientId);
                stmt.setBytes(p++, identityKey.serialize());
                return stmt.executeUpdate() > 0;
            }
        });
    }

    @Override
    public boolean isTrustedIdentity(SignalProtocolAddress address, IdentityKey identityKey, Direction direction) {
        if (trustNewIdentity == TrustNewIdentity.ALWAYS) {
            return true;
        }
        RecipientId recipientId = resolveRecipient(address.getName());
        IdentityInfo identityInfo = transaction(c -> dbLoadIdentity(c, recipientId.getId()));
        if (identityInfo == null) {
            // Identity not found
            return trustNewIdentity == TrustNewIdentity.ON_FIRST_USE;
        }

        // TODO implement possibility for different handling of incoming/outgoing trust decisions
        if (!Objects.equals(identityInfo.getIdentityKey(), identityKey)) {
            // Identity found, but different
            return false;
        }
        return identityInfo.isTrusted();
    }
    
    @Override
    public IdentityKey getIdentity(SignalProtocolAddress address) {
        var recipientId = resolveRecipient(address.getName());
        var identity = getIdentity(recipientId);
        return identity == null ? null : identity.getIdentityKey();
    }

    @Override
    public IdentityInfo getIdentity(RecipientId recipientId) {
        return transaction(c -> dbLoadIdentity(c, recipientId.getId()));
    }

    @Override
    public List<IdentityInfo> getIdentities() {
        return transaction(c -> dbLoadIdentities(c));
    }
    
    private IdentityInfo dbLoadIdentity(Connection connection, long recipientId) throws SQLException {
        String sqlQuery = "SELECT recipient, identity_key, trust_level, date_added FROM identity WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setLong(1, recipientId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return readIdentityInfo(rs);
                }
            }
        }
        return null;
    }
    
    private List<IdentityInfo> dbLoadIdentities(Connection connection) throws SQLException {
        String sqlQuery = "SELECT recipient, identity_key, trust_level, date_added FROM identity";
        List<IdentityInfo> identities = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    identities.add(readIdentityInfo(rs));
                }
            }
        }
        return identities;
    }

    private static IdentityInfo readIdentityInfo(ResultSet rs) throws SQLException {
        int c = 1;
        return new IdentityInfo(RecipientId.of(rs.getLong(c++)),
                identityKey(rs.getBytes(c++)),
                EnumUtil.fromOrdinal(TrustLevel.class, rs.getInt(c++)),
                new Date(rs.getDate(c++).getTime()));
    }
    
    private static IdentityKey identityKey(byte[] bytes) {
        try {
            return new IdentityKey(bytes);
        } catch (InvalidKeyException e) {
            throw new AssertionError("Failed to decode identity key");
        }
    }

    /**
     * @param identifier can be either a serialized uuid or a e164 phone number
     */
    private RecipientId resolveRecipient(String identifier) {
        return resolver.resolveRecipient(identifier);
    }
}
